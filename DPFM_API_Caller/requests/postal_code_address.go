package requests

type PostalCodeAddress struct {
	PostalCode          string `json:"PostalCode"`
	Country             string `json:"Country"`
	IsMarkedForDeletion *bool  `json:"IsMarkedForDeletion"`
}
