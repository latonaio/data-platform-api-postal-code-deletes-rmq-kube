package dpfm_api_output_formatter

import (
	"database/sql"
	"fmt"
)

func ConvertToPostalCode(rows *sql.Rows) (*PostalCode, error) {
	defer rows.Close()
	postalCode := PostalCode{}
	i := 0

	for rows.Next() {
		i++
		err := rows.Scan(
			&postalCode.postalCode,
			&postalCode.country,
			&postalCode.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &postalCode, err
		}

	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &postalCode, nil
	}

	return &postalCode, nil
}

func ConvertToPostalCodeAddress(rows *sql.Rows) (*[]PostalCodeAddress, error) {
	defer rows.Close()
	postalCodeAddress := make([]PostalCodeAddress, 0)
	i := 0

	for rows.Next() {
		i++
		postalCodeAddress := PostalCodeAddress{}
		err := rows.Scan(
			&postalCodeAddress.postalCode,
			&postalCodeAddress.Country,
			&postalCodeAddress.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &postalCodeAddress, err
		}

		postalCodeAddress = append(postalCodeAddress, postalCodeAddress)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &postalCodeAddress, nil
	}

	return &postalCodeAddress, nil
}
