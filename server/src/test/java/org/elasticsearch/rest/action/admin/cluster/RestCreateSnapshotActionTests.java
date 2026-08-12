/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;

public class RestCreateSnapshotActionTests extends ESTestCase {

    /**
     * The request body may carry a plaintext {@code encrypted_data_password}; it must be redacted from the filtered
     * request that the security audit trail logs when {@code emit_request_body} is enabled.
     */
    public void testEncryptedDataPasswordIsFilteredFromAuditedBody() throws Exception {
        BytesReference body = new BytesArray("""
            {
              "indices": ["data-*"],
              "include_global_state": true,
              "encrypted_data_password": "super-secret-snapshot-password",
              "encrypted_data_password_id": "my-password-id"
            }""");
        RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(body, XContentType.JSON).build();

        RestRequest filtered = new RestCreateSnapshotAction().getFilteredRequest(request);

        assertNotEquals(body, filtered.content());
        Map<String, Object> map = XContentHelper.convertToMap(filtered.content(), false, XContentType.JSON).v2();
        assertNull(map.get("encrypted_data_password"));
        // the rest of the body must survive filtering, including the non-sensitive password id
        assertEquals(true, map.get("include_global_state"));
        assertEquals("my-password-id", map.get("encrypted_data_password_id"));
        assertFalse(filtered.content().utf8ToString().contains("super-secret-snapshot-password"));
    }
}
