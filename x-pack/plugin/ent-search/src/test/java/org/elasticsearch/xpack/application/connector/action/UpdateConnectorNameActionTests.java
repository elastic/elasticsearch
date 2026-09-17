/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.application.connector.Connector;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UpdateConnectorNameActionTests extends ESTestCase {

    public void testValidate_WhenNameAndDescriptionPresent_ExpectNoValidationError() {
        UpdateConnectorNameAction.Request request = new UpdateConnectorNameAction.Request(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10)
        );
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, nullValue());
    }

    public void testValidate_WhenConnectorIdIsEmpty_ExpectValidationError() {
        UpdateConnectorNameAction.Request requestWithEmptyConnectorId = new UpdateConnectorNameAction.Request(
            "",
            randomAlphaOfLength(10),
            randomAlphaOfLength(10)
        );
        ActionRequestValidationException exception = requestWithEmptyConnectorId.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString("[connector_id] cannot be [null] or [\"\"]"));
    }

    public void testValidate_WhenNameAndDescriptionAreNull_ExpectValidationError() {
        UpdateConnectorNameAction.Request requestWithMissingFields = new UpdateConnectorNameAction.Request(
            randomAlphaOfLength(10),
            null,
            null
        );
        ActionRequestValidationException exception = requestWithMissingFields.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString("[name] and [description] cannot both be [null]"));
    }

    public void testValidate_WhenDescriptionAtMaxLength_ExpectNoValidationError() {
        UpdateConnectorNameAction.Request request = new UpdateConnectorNameAction.Request(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(Connector.MAX_DESCRIPTION_LENGTH)
        );
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, nullValue());
    }

    public void testValidate_WhenDescriptionExceedsMaxLength_ExpectValidationError() {
        UpdateConnectorNameAction.Request requestWithTooLongDescription = new UpdateConnectorNameAction.Request(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(Connector.MAX_DESCRIPTION_LENGTH + 1)
        );
        ActionRequestValidationException exception = requestWithTooLongDescription.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString("[description] length [" + (Connector.MAX_DESCRIPTION_LENGTH + 1) + "]"));
        assertThat(exception.getMessage(), containsString("maximum allowed length [" + Connector.MAX_DESCRIPTION_LENGTH + "]"));
    }
}
