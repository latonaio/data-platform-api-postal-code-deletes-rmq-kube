package requests

type PostalCode struct {
	PostalCode          string `json:"PostalCode"`
	Country             string `json:"Country"`
	IsMarkedForDeletion *bool  `json:"IsMarkedForDeletion"`
}
