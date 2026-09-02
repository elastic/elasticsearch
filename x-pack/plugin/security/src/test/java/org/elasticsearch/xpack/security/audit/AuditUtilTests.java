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
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
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
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

public class AuditUtilTests extends ESTestCase {

    public void testRestRequestContentExceedsLimitThrows() {
        String json = "{\"key\":\"value\"}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(json.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        ).build();
        var limiter = new RequestBodyRenderer(json.length() - 1, null, null);
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> AuditUtil.restRequestContent(request, "xpack.security.audit.logfile.events.max_request_body_size", limiter)
        );
        assertThat(ex.status(), is(RestStatus.REQUEST_ENTITY_TOO_LARGE));
    }

    public void testRestRequestContentWithinLimitReturnsJson() {
        String json = "{\"key\":\"value\"}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(json.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        ).build();
        try (var limiter = new RequestBodyRenderer(json.length(), null, null)) {
            assertEquals(json, AuditUtil.restRequestContent(request, "setting.key", limiter));
        }
    }

    public void testRestRequestContentZeroLimitIsUnlimited() {
        String json = "{\"key\":\"value\"}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(json.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        ).build();
        try (var limiter = new RequestBodyRenderer(0, null, null)) {
            assertEquals(json, AuditUtil.restRequestContent(request, null, limiter));
        }
    }

    public void testRestRequestContentSmileLimitEnforcedDuringRendering() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(buildSmileBytes(50, "longfieldname_", "longvalue_")),
            XContentType.SMILE
        ).build();

        var tinyLimiter = new RequestBodyRenderer(10, null, null);
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> AuditUtil.restRequestContent(request, "xpack.security.audit.logfile.events.max_request_body_size", tinyLimiter)
        );
        assertThat(ex.status(), is(RestStatus.REQUEST_ENTITY_TOO_LARGE));

        try (var limiter = new RequestBodyRenderer(0, null, null)) {
            String json = AuditUtil.restRequestContent(request, null, limiter);
            assertTrue(json.contains("longfieldname_0"));
        }
    }

    public void testRestRequestContentCircuitBreakerTripsOnRendering() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(buildSmileBytes(10, "key_", "value_")),
            XContentType.SMILE
        ).build();

        // NoopCircuitBreaker is a no-op for every method we don't override; only addEstimateBytesAndMaybeBreak needs a real behavior.
        CircuitBreaker trippingBreaker = new NoopCircuitBreaker("test") {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                throw new CircuitBreakingException("test breaker tripped", Durability.TRANSIENT);
            }
        };

        var limiter = new RequestBodyRenderer(0, trippingBreaker, "test");
        expectThrows(CircuitBreakingException.class, () -> AuditUtil.restRequestContent(request, null, limiter));
        limiter.close();
    }

    public void testRestRequestContentReleasesBreakerAfterSuccessfulRendering() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(buildSmileBytes(10, "field_", "value_")),
            XContentType.SMILE
        ).build();

        AtomicLong used = new AtomicLong();
        CircuitBreaker counting = new NoopCircuitBreaker("test") {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
                used.addAndGet(bytes);
            }

            @Override
            public void addWithoutBreaking(long bytes) {
                used.addAndGet(bytes);
            }
        };

        try (var limiter = new RequestBodyRenderer(0, counting, "test")) {
            String json = AuditUtil.restRequestContent(request, null, limiter);
            assertTrue(json.contains("field_0"));
        }
        assertEquals("breaker must be balanced after successful rendering", 0L, used.get());
    }

    public void testRestRequestContentReleasesBreakerWhenLimitTrips() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(buildSmileBytes(50, "longfield_", "longvalue_")),
            XContentType.SMILE
        ).build();

        AtomicLong used = new AtomicLong();
        CircuitBreaker counting = new NoopCircuitBreaker("test") {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
                used.addAndGet(bytes);
            }

            @Override
            public void addWithoutBreaking(long bytes) {
                used.addAndGet(bytes);
            }
        };

        var limiter = new RequestBodyRenderer(10, counting, "test");
        expectThrows(
            ElasticsearchStatusException.class,
            () -> AuditUtil.restRequestContent(request, "xpack.security.audit.logfile.events.max_request_body_size", limiter)
        );
        limiter.close();
        assertEquals("breaker must be balanced even when the size limit trips", 0L, used.get());
    }

    public void testRestRequestContentNullXContentType() {
        // Protobuf handlers set XContentType to null; restRequestContent must return a placeholder, not throw.
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(new byte[] { 0x0A, 0x02 }), null)
            .withHeaders(Map.of("Content-Type", List.of("application/x-protobuf")))
            .build();
        try (var limiter = new RequestBodyRenderer(0, null, null)) {
            assertThat(AuditUtil.restRequestContent(request, null, limiter), containsString("Unrecognized content type"));
        }
    }

    public void testRestRequestContentInvalidBodyReturnsInvalidFormat() {
        // Malformed YAML body: convertToJson throws; must not propagate, must return "Invalid Format".
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("key: [unclosed".getBytes(StandardCharsets.UTF_8)),
            XContentType.YAML
        ).build();
        try (var limiter = new RequestBodyRenderer(0, null, null)) {
            assertThat(AuditUtil.restRequestContent(request, null, limiter), containsString("Invalid Format"));
        }
    }

    private static byte[] buildSmileBytes(int fields, String keyPrefix, String valuePrefix) throws Exception {
        try (XContentBuilder smileBuilder = XContentFactory.smileBuilder()) {
            smileBuilder.startObject();
            for (int i = 0; i < fields; i++) {
                smileBuilder.field(keyPrefix + i, valuePrefix + i);
            }
            smileBuilder.endObject();
            return BytesReference.toBytes(BytesReference.bytes(smileBuilder));
        }
    }
    public void testHasProtobufContent() {
        assertTrue(AuditUtil.hasProtobufContent(protobufRequest(randomByteArrayOfLength(randomIntBetween(1, 32)))));

        RestRequest jsonRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{}".getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        ).build();
        assertFalse(AuditUtil.hasProtobufContent(jsonRequest));

        RestRequest noContentTypeRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(randomByteArrayOfLength(randomIntBetween(1, 32))),
            null
        ).build();
        assertFalse(AuditUtil.hasProtobufContent(noContentTypeRequest));

        RestRequest noContentRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", List.of("application/x-protobuf"))
        ).build();
        assertFalse(AuditUtil.hasProtobufContent(noContentRequest));
    }

    public void testRestRequestRawContentReturnsBase64() {
        byte[] body = randomByteArrayOfLength(randomIntBetween(1, 64));
        String encoded = AuditUtil.restRequestRawContent(protobufRequest(body), 0, null);
        assertArrayEquals(body, Base64.getDecoder().decode(encoded));
    }

    public void testRestRequestRawContentAssertsContentPresent() {
        RestRequest empty = new FakeRestRequest.Builder(xContentRegistry()).build();
        expectThrows(AssertionError.class, () -> AuditUtil.restRequestRawContent(empty, 0, null));
    }

    public void testRestRequestRawContentExceedsLimitThrows() {
        byte[] body = randomByteArrayOfLength(randomIntBetween(9, 64));
        int encodedLength = Math.toIntExact(AuditUtil.base64EncodedLength(body.length));
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> AuditUtil.restRequestRawContent(
                protobufRequest(body),
                encodedLength - 1,
                "xpack.security.audit.logfile.events.max_request_body_size"
            )
        );
        assertThat(ex.status(), is(RestStatus.REQUEST_ENTITY_TOO_LARGE));
        assertThat(ex.getMessage(), containsString("xpack.security.audit.logfile.events.max_request_body_size"));
    }

    public void testRestRequestRawContentWithinLimit() {
        byte[] body = randomByteArrayOfLength(randomIntBetween(1, 64));
        int encodedLength = Math.toIntExact(AuditUtil.base64EncodedLength(body.length));
        assertEquals(
            Base64.getEncoder().encodeToString(body),
            AuditUtil.restRequestRawContent(protobufRequest(body), encodedLength, "setting.key")
        );
    }

    private RestRequest protobufRequest(byte[] body) {
        // media type matching must ignore case and parameters
        String contentType = randomFrom("application/x-protobuf", "Application/X-Protobuf", "application/x-protobuf; charset=UTF-8");
        return new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(body), null)
            .withHeaders(Map.of("Content-Type", List.of(contentType)))
            .build();
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
