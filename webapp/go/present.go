package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
)

// listPresent プレゼント一覧
// GET /user/{userID}/present/index/{n}
func (h *Handler) listPresent(c echo.Context) error {
	n, err := strconv.Atoi(c.Param("n"))
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, fmt.Errorf("invalid index number (n) parameter"))
	}
	if n == 0 {
		return errorResponse(c, http.StatusBadRequest, fmt.Errorf("index number (n) should be more than or equal to 1"))
	}

	userID, err := getUserID(c)
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, fmt.Errorf("invalid userID parameter"))
	}

	offset := PresentCountPerPage * (n - 1)
	presentList := []*UserPresent{}
	query := `
	SELECT * FROM user_presents 
	WHERE user_id = ? AND deleted_at IS NULL
	ORDER BY created_at DESC, id
	LIMIT ? OFFSET ?`
	if err = h.DB.Select(&presentList, query, userID, PresentCountPerPage, offset); err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}

	var presentCount int
	if err = h.DB.Get(&presentCount, "SELECT COUNT(*) FROM user_presents WHERE user_id = ? AND deleted_at IS NULL", userID); err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}

	isNext := false
	if presentCount > (offset + PresentCountPerPage) {
		isNext = true
	}

	return successResponse(c, &ListPresentResponse{
		Presents: presentList,
		IsNext:   isNext,
	})
}

type ListPresentResponse struct {
	Presents []*UserPresent `json:"presents"`
	IsNext   bool           `json:"isNext"`
}

// receivePresent プレゼント受け取り
// POST /user/{userID}/present/receive
func (h *Handler) receivePresent(c echo.Context) error {
	defer c.Request().Body.Close()
	req := new(ReceivePresentRequest)
	if err := parseRequestBody(c, req); err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}

	userID, err := getUserID(c)
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}

	requestAt, err := getRequestTime(c)
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, ErrGetRequestTime)
	}

	if len(req.PresentIDs) == 0 {
		return errorResponse(c, http.StatusUnprocessableEntity, fmt.Errorf("presentIds is empty"))
	}

	if err = h.checkViewerID(userID, req.ViewerID); err != nil {
		if err == ErrUserDeviceNotFound {
			return errorResponse(c, http.StatusNotFound, err)
		}
		return errorResponse(c, http.StatusInternalServerError, err)
	}

	// 未取得のプレゼント取得
	query := "SELECT * FROM user_presents WHERE id IN (?) AND deleted_at IS NULL"
	query, params, err := sqlx.In(query, req.PresentIDs)
	if err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}
	obtainPresent := []*UserPresent{}
	if err = h.DB.Select(&obtainPresent, query, params...); err != nil {
		return errorResponse(c, http.StatusBadRequest, err)
	}

	if len(obtainPresent) == 0 {
		return successResponse(c, &ReceivePresentResponse{
			UpdatedResources: makeUpdatedResources(requestAt, nil, nil, nil, nil, nil, nil, []*UserPresent{}),
		})
	}

	tx, err := h.DB.Beginx()
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	defer tx.Rollback() //nolint:errcheck

	userPresentIDs := make([]int64, 0, len(obtainPresent))
	for _, v := range obtainPresent {
		userPresentIDs = append(userPresentIDs, v.ID)
	}
	query, params, err = sqlx.In("UPDATE user_presents SET deleted_at=?, updated_at=? WHERE id IN (?)", requestAt, requestAt, userPresentIDs)
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}
	if _, err := tx.Exec(query, params...); err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}

	// 配布処理
	for i := range obtainPresent {
		if obtainPresent[i].DeletedAt != nil {
			return errorResponse(c, http.StatusInternalServerError, fmt.Errorf("received present"))
		}

		obtainPresent[i].UpdatedAt = requestAt
		obtainPresent[i].DeletedAt = &requestAt
		v := obtainPresent[i]

		_, _, _, err = h.obtainItem(tx, v.UserID, v.ItemID, v.ItemType, int64(v.Amount), requestAt)
		if err != nil {
			if err == ErrUserNotFound || err == ErrItemNotFound {
				return errorResponse(c, http.StatusNotFound, err)
			}
			if err == ErrInvalidItemType {
				return errorResponse(c, http.StatusBadRequest, err)
			}
			return errorResponse(c, http.StatusInternalServerError, err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return errorResponse(c, http.StatusInternalServerError, err)
	}

	return successResponse(c, &ReceivePresentResponse{
		UpdatedResources: makeUpdatedResources(requestAt, nil, nil, nil, nil, nil, nil, obtainPresent),
	})
}

type ReceivePresentRequest struct {
	ViewerID   string  `json:"viewerId"`
	PresentIDs []int64 `json:"presentIds"`
}

type ReceivePresentResponse struct {
	UpdatedResources *UpdatedResource `json:"updatedResources"`
}

// obtainPresent プレゼント付与
func (h *Handler) obtainPresent(tx *sqlx.Tx, userID int64, requestAt int64) ([]*UserPresent, error) {
	normalPresents := make([]*PresentAllMaster, 0)
	query := "SELECT * FROM present_all_masters WHERE registered_start_at <= ? AND registered_end_at >= ?"
	if err := tx.Select(&normalPresents, query, requestAt, requestAt); err != nil {
		return nil, err
	}

	normalPresentIDs := make([]int64, 0, len(normalPresents))
	for _, np := range normalPresents {
		normalPresentIDs = append(normalPresentIDs, np.ID)
	}

	receivedIDs := make([]int64, 0, len(normalPresents))
	query, params, err := sqlx.In("SELECT present_all_id FROM user_present_all_received_history WHERE user_id=? AND present_all_id IN (?)", userID, normalPresentIDs)
	if err != nil {
		return nil, err
	}
	err = tx.Select(receivedIDs, query, params...)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	notReceivedNormalPresents := make([]*PresentAllMaster, 0, len(normalPresents))
	if err == sql.ErrNoRows {
		notReceivedNormalPresents = normalPresents
	} else {
		receivedMap := make(map[int64]struct{}, len(receivedIDs))
		for _, r := range receivedIDs {
			receivedMap[r] = struct{}{}
		}

		for _, np := range normalPresents {
			if _, ok := receivedMap[np.ID]; !ok {
				notReceivedNormalPresents = append(notReceivedNormalPresents, np)
			}
		}
	}

	obtainPresents := make([]*UserPresent, 0, len(notReceivedNormalPresents))
	histories := make([]*UserPresentAllReceivedHistory, 0, len(notReceivedNormalPresents))
	for _, np := range notReceivedNormalPresents {
		up := &UserPresent{
			UserID:         userID,
			SentAt:         requestAt,
			ItemType:       np.ItemType,
			ItemID:         np.ItemID,
			Amount:         int(np.Amount),
			PresentMessage: np.PresentMessage,
			CreatedAt:      requestAt,
			UpdatedAt:      requestAt,
		}
		obtainPresents = append(obtainPresents, up)

		history := &UserPresentAllReceivedHistory{
			UserID:       userID,
			PresentAllID: np.ID,
			ReceivedAt:   requestAt,
			CreatedAt:    requestAt,
			UpdatedAt:    requestAt,
		}
		histories = append(histories, history)
	}

	query = "INSERT INTO user_presents(user_id, sent_at, item_type, item_id, amount, present_message, created_at, updated_at) VALUES (:user_id, :sent_at, :item_type, :item_id, :amount, :present_message, :created_at, :updated_at)"
	if _, err := tx.Exec(query, obtainPresents); err != nil {
		return nil, err
	}

	query = "INSERT INTO user_present_all_received_history(user_id, present_all_id, received_at, created_at, updated_at) VALUES (:user_id, :present_all_id, :received_at, :created_at, :updated_at)"
	if _, err = tx.Exec(query, histories); err != nil {
		return nil, err
	}

	return obtainPresents, nil
}
