/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with,
 * at your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.http;

import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;

import java.nio.charset.StandardCharsets;

public class SmileBodyValidationIT extends AbstractHttpSmokeTestIT {

    /**
     * Sending bytes that are not Smile-encoded with {@code Content-Type: application/smile}
     * must surface as a 4XX client error rather than a 5XX or a silent success. This
     * complements the unit-level coverage in {@code SmileXContentImplTests} by
     * exercising the full REST request path.
     */
    public void testSmileContentTypeRejectsNonSmileBody() {
        Request request = new Request("POST", "/_search");
        request.setEntity(
            new ByteArrayEntity("{\"query\":{\"match_all\":{}}}".getBytes(StandardCharsets.UTF_8), ContentType.create("application/smile"))
        );
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
    }
}
