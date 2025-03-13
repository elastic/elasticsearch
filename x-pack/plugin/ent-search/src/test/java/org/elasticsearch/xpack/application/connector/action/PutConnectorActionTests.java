/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PutConnectorActionTests extends ESTestCase {

    public void testValidate_WhenConnectorIdAndIndexNamePresent_ExpectNoValidationError() {
        PutConnectorAction.Request request = new PutConnectorAction.Request(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            false,
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10)
        );
        ActionRequestValidationException exception = request.validate();

        assertThat(exception, nullValue());
    }

    public void testValidate_WrongIndexNamePresentForManagedConnector_ExpectValidationError() {
        PutConnectorAction.Request requestWithIllegalIndexName = new PutConnectorAction.Request(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            "wrong-prefix-" + randomAlphaOfLength(10),
            true,
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10)
        );
        ActionRequestValidationException exception = requestWithIllegalIndexName.validate();

        assertThat(exception, notNullValue());
        assertThat(
            exception.getMessage(),
            containsString("Index attached to an Elastic-managed connector must start with the prefix: [content-]")
        );
    }

    public void testValidate_WhenMalformedIndexName_ExpectValidationError() {
        PutConnectorAction.Request requestWithMissingConnectorId = new PutConnectorAction.Request(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            "_illegal-index-name",
            randomBoolean(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10)
        );
        ActionRequestValidationException exception = requestWithMissingConnectorId.validate();

        assertThat(exception, notNullValue());
        assertThat(exception.getMessage(), containsString("Invalid index name [_illegal-index-name]"));
    }
}
