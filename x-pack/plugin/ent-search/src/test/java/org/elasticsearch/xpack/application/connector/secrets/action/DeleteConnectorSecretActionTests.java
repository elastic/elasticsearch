/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.application.connector.secrets.ConnectorSecretsConstants;
import org.elasticsearch.xpack.application.connector.secrets.ConnectorSecretsTestUtils;

import static org.elasticsearch.xpack.application.connector.ConnectorTestUtils.NULL_STRING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DeleteConnectorSecretActionTests extends ESTestCase {

    public void testValidate_WhenConnectorSecretIdIsPresent_ExpectNoValidationError() {
        DeleteConnectorSecretRequest request = ConnectorSecretsTestUtils.getRandomDeleteConnectorSecretRequest();
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, nullValue());
    }

    public void testValidate_WhenConnectorSecretIdIsEmpty_ExpectValidationError() {
        DeleteConnectorSecretRequest requestWithMissingConnectorId = new DeleteConnectorSecretRequest("");
        ActionRequestValidationException exception = requestWithMissingConnectorId.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(ConnectorSecretsConstants.CONNECTOR_SECRET_ID_NULL_OR_EMPTY_MESSAGE));
    }

    public void testValidate_WhenConnectorSecretIdIsNull_ExpectValidationError() {
        DeleteConnectorSecretRequest requestWithMissingConnectorId = new DeleteConnectorSecretRequest(NULL_STRING);
        ActionRequestValidationException exception = requestWithMissingConnectorId.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(ConnectorSecretsConstants.CONNECTOR_SECRET_ID_NULL_OR_EMPTY_MESSAGE));
    }
}
