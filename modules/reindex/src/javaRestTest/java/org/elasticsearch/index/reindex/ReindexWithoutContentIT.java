/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;

public class ReindexWithoutContentIT extends ESRestTestCase {

    public void testReindexMissingBody() throws IOException {
        ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("POST", "/_reindex"))
        );
        assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
        assertThat(responseException.getMessage(), containsString("request body is required"));
    }
}
