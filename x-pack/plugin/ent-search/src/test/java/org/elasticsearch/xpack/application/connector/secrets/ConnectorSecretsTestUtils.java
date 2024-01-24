/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets;

import org.elasticsearch.xpack.application.connector.secrets.action.GetConnectorSecretRequest;
import org.elasticsearch.xpack.application.connector.secrets.action.GetConnectorSecretResponse;
import org.elasticsearch.xpack.application.connector.secrets.action.PostConnectorSecretRequest;
import org.elasticsearch.xpack.application.connector.secrets.action.PostConnectorSecretResponse;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;

public class ConnectorSecretsTestUtils {

    public static GetConnectorSecretRequest getRandomGetConnectorSecretRequest() {
        return new GetConnectorSecretRequest(randomAlphaOfLength(10));
    }

    public static GetConnectorSecretResponse getRandomGetConnectorSecretResponse() {
        final String id = randomAlphaOfLength(10);
        final String value = randomAlphaOfLength(10);
        return new GetConnectorSecretResponse(id, value);
    }

    public static PostConnectorSecretRequest getRandomPostConnectorSecretRequest() {
        return new PostConnectorSecretRequest(randomAlphaOfLengthBetween(0, 20));
    }

    public static PostConnectorSecretResponse getRandomPostConnectorSecretResponse() {
        return new PostConnectorSecretResponse(randomAlphaOfLength(10));
    }
}
