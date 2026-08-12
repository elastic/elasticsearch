/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;

public class RestPutSnapshotLifecycleActionTests extends ESTestCase {

    /**
     * The request body may carry a plaintext {@code encrypted_data_password} nested in the policy's {@code config};
     * it must be redacted from the filtered request that the security audit trail logs.
     */
    public void testEncryptionPasswordIsFilteredFromAuditedBody() throws Exception {
        BytesReference body = new BytesArray("""
            {
              "schedule": "0 30 1 * * ?",
              "name": "<daily-snap-{now/d}>",
              "repository": "my_repository",
              "config": {
                "indices": ["data-*"],
                "include_global_state": true,
                "encrypted_data_password": "super-secret-snapshot-password"
              }
            }""");
        RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(body, XContentType.JSON).build();

        RestRequest filtered = new RestPutSnapshotLifecycleAction().getFilteredRequest(request);

        assertNotEquals(body, filtered.content());
        Map<String, Object> map = XContentHelper.convertToMap(filtered.content(), false, XContentType.JSON).v2();
        @SuppressWarnings("unchecked")
        Map<String, Object> config = (Map<String, Object>) map.get("config");
        assertNull(config.get("encrypted_data_password"));
        // the rest of the body must survive filtering
        assertEquals("0 30 1 * * ?", map.get("schedule"));
        assertEquals("my_repository", map.get("repository"));
        assertEquals(true, config.get("include_global_state"));
        assertFalse(filtered.content().utf8ToString().contains("super-secret-snapshot-password"));
    }
}
