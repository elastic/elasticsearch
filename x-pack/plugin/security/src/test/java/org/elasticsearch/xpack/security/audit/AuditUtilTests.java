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
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for the audit utils class
 */
public class AuditUtilTests extends ESTestCase {

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

    public void testRestRequestContentNullXContentType() {
        // Protobuf handlers set XContentType to null; restRequestContent must return a placeholder, not throw.
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(new byte[] { 0x0A, 0x02 }), null)
            .withHeaders(Map.of("Content-Type", List.of("application/x-protobuf")))
            .build();
        assertThat(AuditUtil.restRequestContent(request, 0, null), containsString("Unrecognized content type"));
    }

    public void testRestRequestContentInvalidBodyReturnsInvalidFormat() {
        // Malformed YAML body: convertToJson throws; must not propagate, must return "Invalid Format".
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("key: [unclosed".getBytes(StandardCharsets.UTF_8)),
            XContentType.YAML
        ).build();
        assertThat(AuditUtil.restRequestContent(request, 0, null), containsString("Invalid Format"));
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
