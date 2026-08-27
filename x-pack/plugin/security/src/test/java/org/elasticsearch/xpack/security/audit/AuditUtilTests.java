/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.MockIndicesRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.XContentType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for the audit utils class
 */
public class AuditUtilTests extends ESTestCase {

    // ── restRequestContent with JSON-length size limit ────────────────────────

    public void testRestRequestContentExceedsLimitThrows() {
        String json = "{\"key\":\"value\"}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(json.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        ).build();
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> AuditUtil.restRequestContent(request, json.length() - 1, "xpack.security.audit.logfile.events.max_request_body_size")
        );
        assertThat(ex.status(), is(RestStatus.REQUEST_ENTITY_TOO_LARGE));
    }

    public void testRestRequestContentWithinLimitReturnsJson() {
        String json = "{\"key\":\"value\"}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(json.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        ).build();
        assertEquals(json, AuditUtil.restRequestContent(request, json.length(), "setting.key"));
    }

    public void testRestRequestContentZeroLimitIsUnlimited() {
        String json = "{\"key\":\"value\"}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(json.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        ).build();
        assertEquals(json, AuditUtil.restRequestContent(request, 0, null));
    }

    // ── indices ───────────────────────────────────────────────────────────────

    public void testIndicesRequest() {
        assertNull(AuditUtil.indices(new MockIndicesRequest(null, (String[]) null)));
        final int numberOfIndices = randomIntBetween(1, 100);
        List<String> expectedIndices = new ArrayList<>();
        final boolean includeDuplicates = randomBoolean();
        for (int i = 0; i < numberOfIndices; i++) {
            String name = randomAlphaOfLengthBetween(1, 30);
            expectedIndices.add(name);
            if (includeDuplicates) {
                expectedIndices.add(name);
            }
        }
        final Set<String> uniqueExpectedIndices = new HashSet<>(expectedIndices);
        final Set<String> result = AuditUtil.indices(
            new MockIndicesRequest(null, expectedIndices.toArray(new String[expectedIndices.size()]))
        );
        assertNotNull(result);
        assertEquals(uniqueExpectedIndices.size(), result.size());
        assertThat(result, hasItems(uniqueExpectedIndices.toArray(Strings.EMPTY_ARRAY)));
    }
}
