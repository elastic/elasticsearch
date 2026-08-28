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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

public class AuditUtilTests extends ESTestCase {

    public void testRestRequestContentExceedsLimitThrows() {
        String json = "{\"key\":\"value\"}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(json.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        ).build();
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> AuditUtil.restRequestContent(
                request,
                json.length() - 1,
                null,
                "xpack.security.audit.logfile.events.max_request_body_size"
            )
        );
        assertThat(ex.status(), is(RestStatus.REQUEST_ENTITY_TOO_LARGE));
    }

    public void testRestRequestContentWithinLimitReturnsJson() {
        String json = "{\"key\":\"value\"}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(json.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        ).build();
        assertEquals(json, AuditUtil.restRequestContent(request, json.length(), null, "setting.key"));
    }

    public void testRestRequestContentZeroLimitIsUnlimited() {
        String json = "{\"key\":\"value\"}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(json.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        ).build();
        assertEquals(json, AuditUtil.restRequestContent(request, 0, null, null));
    }

    public void testRestRequestContentSmileLimitEnforcedDuringRendering() throws Exception {
        XContentBuilder smileBuilder = XContentFactory.smileBuilder().startObject();
        for (int i = 0; i < 50; i++) {
            smileBuilder.field("longfieldname_" + i, "longvalue_" + i);
        }
        smileBuilder.endObject();
        byte[] smileBytes = BytesReference.toBytes(BytesReference.bytes(smileBuilder));

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(smileBytes), XContentType.SMILE)
            .build();

        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> AuditUtil.restRequestContent(request, 10, null, "xpack.security.audit.logfile.events.max_request_body_size")
        );
        assertThat(ex.status(), is(RestStatus.REQUEST_ENTITY_TOO_LARGE));

        String json = AuditUtil.restRequestContent(request, 0, null, null);
        assertTrue(json.contains("longfieldname_0"));
    }

    public void testRestRequestContentCircuitBreakerTripsOnRendering() throws Exception {
        XContentBuilder smileBuilder = XContentFactory.smileBuilder().startObject();
        for (int i = 0; i < 10; i++) {
            smileBuilder.field("key_" + i, "value_" + i);
        }
        smileBuilder.endObject();
        byte[] smileBytes = BytesReference.toBytes(BytesReference.bytes(smileBuilder));
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(smileBytes), XContentType.SMILE)
            .build();

        CircuitBreaker trippingBreaker = new CircuitBreaker() {
            @Override
            public void circuitBreak(String fieldName, long bytesNeeded) {}

            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                throw new CircuitBreakingException("test breaker tripped", Durability.TRANSIENT);
            }

            @Override
            public void addWithoutBreaking(long bytes) {}

            @Override
            public long getUsed() {
                return 0;
            }

            @Override
            public long getLimit() {
                return 0;
            }

            @Override
            public double getOverhead() {
                return 1.0;
            }

            @Override
            public long getTrippedCount() {
                return 0;
            }

            @Override
            public String getName() {
                return "test";
            }

            @Override
            public Durability getDurability() {
                return Durability.TRANSIENT;
            }

            @Override
            public void setLimitAndOverhead(long limit, double overhead) {}
        };

        expectThrows(CircuitBreakingException.class, () -> AuditUtil.restRequestContent(request, 0, trippingBreaker, null));
    }

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
