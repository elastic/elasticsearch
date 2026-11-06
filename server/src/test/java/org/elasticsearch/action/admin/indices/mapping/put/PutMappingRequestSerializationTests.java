/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.mapping.put;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class PutMappingRequestSerializationTests extends AbstractWireSerializingTestCase<PutMappingRequest> {

    @Override
    protected Writeable.Reader<PutMappingRequest> instanceReader() {
        return PutMappingRequest::new;
    }

    @Override
    protected PutMappingRequest createTestInstance() {
        PutMappingRequest request = new PutMappingRequest(generateRandomStringArray(3, 10, false, false));
        XContentType xContentType = randomFrom(XContentType.JSON, XContentType.CBOR, XContentType.SMILE);
        request.source(randomMappingSource(xContentType), xContentType);
        if (randomBoolean()) {
            request.origin(randomAlphaOfLength(10));
        }
        request.writeIndexOnly(randomBoolean());
        return request;
    }

    @Override
    protected PutMappingRequest mutateInstance(PutMappingRequest instance) throws IOException {
        return switch (randomInt(3)) {
            case 0 -> {
                PutMappingRequest mutated = copyInstance(instance);
                mutated.indices(generateRandomStringArray(3, 10, false, false));
                yield mutated;
            }
            case 1 -> {
                PutMappingRequest mutated = copyInstance(instance);
                XContentType xContentType = randomFrom(XContentType.JSON, XContentType.CBOR, XContentType.SMILE);
                mutated.source(randomMappingSource(xContentType), xContentType);
                yield mutated;
            }
            case 2 -> {
                PutMappingRequest mutated = copyInstance(instance);
                mutated.writeIndexOnly(instance.writeIndexOnly() == false);
                yield mutated;
            }
            default -> {
                PutMappingRequest mutated = copyInstance(instance);
                mutated.origin(randomValueOtherThan(instance.origin(), () -> randomAlphaOfLength(10)));
                yield mutated;
            }
        };
    }

    /**
     * Verifies that non-JSON content is converted to JSON when serialized to a node that does not
     * support {@link PutMappingRequest#MAPPINGS_AS_BYTESREFERENCE}, so that old nodes always receive
     * a JSON string regardless of the original format.
     */
    public void testBwcSerializationConvertsToJson() throws IOException {
        XContentType xContentType = randomFrom(XContentType.CBOR, XContentType.SMILE);
        PutMappingRequest request = new PutMappingRequest("test-index");
        request.source(randomMappingSource(xContentType), xContentType);

        PutMappingRequest deserialized = copyInstance(
            request,
            TransportVersionUtils.randomVersionNotSupporting(PutMappingRequest.MAPPINGS_AS_BYTESREFERENCE)
        );
        assertEquals(XContentType.JSON, deserialized.xContentType());
        assertTrue(deserialized.source().utf8ToString().startsWith("{"));
    }

    private static BytesReference randomMappingSource(XContentType xContentType) {
        try (XContentBuilder builder = XContentFactory.contentBuilder(xContentType)) {
            builder.startObject();
            builder.startObject("properties");
            builder.startObject(randomAlphaOfLength(5));
            builder.field("type", "keyword");
            builder.endObject();
            builder.endObject();
            builder.endObject();
            return BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
