package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-postal-code-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-postal-code-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) PostalCodeRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.PostalCode {
	where := fmt.Sprintf("WHERE postalCode.PostalCode = %d ", input.PostalCode.PostalCode)
	rows, err := c.db.Query(
		`SELECT 
			postalCode.PostalCode
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_postal_code_postal_code_data as postalCode 
		` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToPostalCode(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) PostalCodeAddresssRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.PostalCodeAddress {
	where := fmt.Sprintf("WHERE postalCodeAddress.PostalCode IS NOT NULL\nAND postalCode.PostalCode = %d", input.PostalCode.PostalCode)
	rows, err := c.db.Query(
		`SELECT 
			postalCodeAddress.PostalCode, postalCodeAddress.PostalCodeAddress
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_postal_code_postal_code_data as postalCodeAddress
		INNER JOIN DataPlatformMastersAndTransactionsMysqlKube.data_platform_postal_code_address_lot_postal_code_data as postalCode
		ON postalCode.PostalCode = postalCodeAddress.PostalCode ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToPostalCodeAddress(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}
