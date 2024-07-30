/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import static org.elasticsearch.test.ESTestCase.TEST_REQUEST_TIMEOUT;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class StoredScriptIntegTestUtils {
    private StoredScriptIntegTestUtils() {/* no instances */}

    public static void putJsonStoredScript(String id, String jsonContent) {
        putJsonStoredScript(id, new BytesArray(jsonContent));
    }

    public static void putJsonStoredScript(String id, BytesReference jsonContent) {
        assertAcked(
            ESIntegTestCase.safeExecute(
                TransportPutStoredScriptAction.TYPE,
                new PutStoredScriptRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).id(id).content(jsonContent, XContentType.JSON)
            )
        );
    }
}
