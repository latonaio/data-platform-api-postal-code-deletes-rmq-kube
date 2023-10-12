package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-postal-code-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-postal-code-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"data-platform-api-postal-code-deletes-rmq-kube/config"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	database "github.com/latonaio/golang-mysql-network-connector"
	rabbitmq "github.com/latonaio/rabbitmq-golang-client-for-data-platform"
	"golang.org/x/xerrors"
)

type DPFMAPICaller struct {
	ctx  context.Context
	conf *config.Conf
	rmq  *rabbitmq.RabbitmqClient
	db   *database.Mysql
}

func NewDPFMAPICaller(
	conf *config.Conf, rmq *rabbitmq.RabbitmqClient, db *database.Mysql,
) *DPFMAPICaller {
	return &DPFMAPICaller{
		ctx:  context.Background(),
		conf: conf,
		rmq:  rmq,
		db:   db,
	}
}

func (c *DPFMAPICaller) AsyncDeletes(
	accepter []string,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (interface{}, []error) {
	var response interface{}
	if input.APIType == "deletes" {
		response = c.deleteSqlProcess(input, output, accepter, log)
	}

	return response, nil
}

func (c *DPFMAPICaller) deleteSqlProcess(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	log *logger.Logger,
) *dpfm_api_output_formatter.Message {
	var postalCodeData *dpfm_api_output_formatter.PostalCode
	postalCodeAddressData := make([]dpfm_api_output_formatter.PostalCodeAddress, 0)
	for _, a := range accepter {
		switch a {
		case "PostalCode":
			h, i := c.postalCodeDelete(input, output, log)
			postalCodeData = h
			if h == nil || i == nil {
				continue
			}
			postalCodeAddressData = append(postalCodeAddressData, *i...)
		case "PostalCodeAddress":
			i := c.postalCodeAddressDelete(input, output, log)
			if i == nil {
				continue
			}
			postalCodeAddressData = append(postalCodeAddressData, *i...)
		}
	}

	return &dpfm_api_output_formatter.Message{
		PostalCode:        postalCodeData,
		PostalCodeAddress: &postalCodeAddressData,
	}
}

func (c *DPFMAPICaller) postalCodeDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*dpfm_api_output_formatter.PostalCode, *[]dpfm_api_output_formatter.PostalCodeAddress) {
	sessionID := input.RuntimeSessionID
	postalCode := c.PostalCodeRead(input, log)
	postalCode.PostalCode = input.PostalCode.PostalCode
	postalCode.Country = input.PostalCode.Country
	postalCode.IsMarkedForDeletion = input.PostalCode.IsMarkedForDeletion
	res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": postalCode, "function": "PostalCodePostalCode", "runtime_session_id": sessionID})
	if err != nil {
		err = xerrors.Errorf("rmq error: %w", err)
		log.Error("%+v", err)
		return nil, nil
	}
	res.Success()
	if !checkResult(res) {
		output.SQLUpdateResult = getBoolPtr(false)
		output.SQLUpdateError = "PostalCode Data cannot delete"
		return nil, nil
	}

	// postalCodeの削除が取り消された時は子に影響を与えない
	if !*postalCode.IsMarkedForDeletion {
		return postalCode, nil
	}

	postalCodeAddresss := c.PostalCodeAddresssRead(input, log)
	for i := range *postalCodeAddresss {
		(*postalCodeAddresss)[i].IsMarkedForDeletion = input.PostalCode.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*postalCodeAddresss)[i], "function": "PostalCodePostalCodeAddress", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "PostalCodeAddress Data cannot delete"
			return nil, nil
		}
	}

	return postalCode, postalCodeAddresss
}

func (c *DPFMAPICaller) postalCodeAddressDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.PostalCodeAddress {
	sessionID := input.RuntimeSessionID

	postalCodeAddresss := make([]dpfm_api_output_formatter.PostalCodeAddress, 0)
	for _, v := range input.PostalCode.PostalCodeAddress {
		data := dpfm_api_output_formatter.PostalCodeAddress{
			PostalCode:          input.PostalCode.PostalCode,
			PostalCodeAddress:   v.PostalCodeAddress,
			IsMarkedForDeletion: v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{
			"message":            data,
			"function":           "PostalCodePostalCodeAddress",
			"runtime_session_id": sessionID,
		})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "PostalCode PostalCodeAddress Data cannot delete"
			return nil
		}
	}
	// postalCodeAddressがキャンセル取り消しされた場合、postalCodeのキャンセルも取り消す
	if !*input.PostalCode.PostalCodeAddress[0].IsMarkedForDeletion {
		postalCode := c.PostalCodeRead(input, log)
		postalCode.IsMarkedForDeletion = input.PostalCode.PostalCodeAddress[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": postalCode, "function": "PostalCodePostalCode", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "PostalCode Data cannot delete"
			return nil
		}
	}

	return &postalCodeAddresss
}

func checkResult(msg rabbitmq.RabbitmqMessage) bool {
	data := msg.Data()
	d, ok := data["result"]
	if !ok {
		return false
	}
	result, ok := d.(string)
	if !ok {
		return false
	}
	return result == "success"
}

func getBoolPtr(b bool) *bool {
	return &b
}
