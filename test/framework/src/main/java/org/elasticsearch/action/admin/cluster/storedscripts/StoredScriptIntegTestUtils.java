/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.script.StoredScriptSource;
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
        assertAcked(ESIntegTestCase.safeExecute(TransportPutStoredScriptAction.TYPE, newPutStoredScriptTestRequest(id, jsonContent)));
    }

    public static PutStoredScriptRequest newPutStoredScriptTestRequest(String id, String jsonContent) {
        return newPutStoredScriptTestRequest(id, new BytesArray(jsonContent));
    }

    public static PutStoredScriptRequest newPutStoredScriptTestRequest(String id, BytesReference jsonContent) {
        return new PutStoredScriptRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            id,
            null,
            jsonContent,
            XContentType.JSON,
            StoredScriptSource.parse(jsonContent, XContentType.JSON)
        );
    }
}
