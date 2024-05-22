/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.application.connector.secrets.ConnectorSecretsTestUtils;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PutConnectorSecretActionTests extends ESTestCase {

    public void testValidate_WhenConnectorSecretIdIsPresent_ExpectNoValidationError() {
        PutConnectorSecretRequest request = ConnectorSecretsTestUtils.getRandomPutConnectorSecretRequest();
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, nullValue());
    }

    public void testValidate_WhenConnectorSecretIdIsEmpty_ExpectValidationError() {
        PutConnectorSecretRequest requestWithEmptyId = new PutConnectorSecretRequest("", randomAlphaOfLength(10));
        ActionRequestValidationException exception = requestWithEmptyId.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString("[id] cannot be [null] or [\"\"]"));
    }

    public void testValidate_WhenConnectorSecretIdIsNull_ExpectValidationError() {
        PutConnectorSecretRequest requestWithNullId = new PutConnectorSecretRequest(null, randomAlphaOfLength(10));
        ActionRequestValidationException exception = requestWithNullId.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString("[id] cannot be [null] or [\"\"]"));
    }

    public void testValidate_WhenConnectorSecretValueIsEmpty_ExpectValidationError() {
        PutConnectorSecretRequest requestWithEmptyValue = new PutConnectorSecretRequest(randomAlphaOfLength(10), "");
        ActionRequestValidationException exception = requestWithEmptyValue.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString("[value] cannot be [null] or [\"\"]"));
    }

    public void testValidate_WhenConnectorSecretValueIsNull_ExpectValidationError() {
        PutConnectorSecretRequest requestWithEmptyValue = new PutConnectorSecretRequest(randomAlphaOfLength(10), null);
        ActionRequestValidationException exception = requestWithEmptyValue.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString("[value] cannot be [null] or [\"\"]"));
    }
}
