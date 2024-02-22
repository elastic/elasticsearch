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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetConnectorSecretActionTests extends ESTestCase {

    public void testValidate_WhenConnectorSecretIdIsPresent_ExpectNoValidationError() {
        GetConnectorSecretRequest request = ConnectorSecretsTestUtils.getRandomGetConnectorSecretRequest();
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, nullValue());
    }

    public void testValidate_WhenConnectorSecretIdIsEmpty_ExpectValidationError() {
        GetConnectorSecretRequest requestWithMissingConnectorId = new GetConnectorSecretRequest("");
        ActionRequestValidationException exception = requestWithMissingConnectorId.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(ConnectorSecretsConstants.CONNECTOR_SECRET_ID_NULL_OR_EMPTY_MESSAGE));
    }

    public void testValidate_WhenConnectorSecretIdIsNull_ExpectValidationError() {
        GetConnectorSecretRequest requestWithMissingConnectorId = new GetConnectorSecretRequest("");
        ActionRequestValidationException exception = requestWithMissingConnectorId.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString(ConnectorSecretsConstants.CONNECTOR_SECRET_ID_NULL_OR_EMPTY_MESSAGE));
    }
}
