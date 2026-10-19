/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.mapping.put;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class PutMappingRequestSerializationTests extends ESTestCase {

    public void testRoundTripOnNewAndOldWireFormats() throws IOException {
        for (TransportVersion version : new TransportVersion[] {
            TransportVersion.current(),
            PutMappingRequest.MAPPINGS_AS_BYTESREFERENCE,
            TransportVersionUtils.randomVersionNotSupporting(random(), PutMappingRequest.MAPPINGS_AS_BYTESREFERENCE) }) {
            XContentType xContentType = randomFrom(XContentType.JSON, XContentType.CBOR, XContentType.SMILE);
            PutMappingRequest request = new PutMappingRequest(generateRandomStringArray(3, 10, false, false));
            request.source(randomMappingSource(xContentType), xContentType);
            request.origin(randomAlphaOfLength(10));
            request.writeIndexOnly(randomBoolean());

            PutMappingRequest copy = roundTrip(request, version);
            assertEquals(request.source(), copy.source());
            assertArrayEquals(request.indices(), copy.indices());
            assertEquals(request.origin(), copy.origin());
            assertEquals(request.writeIndexOnly(), copy.writeIndexOnly());
        }
    }

    private static PutMappingRequest roundTrip(PutMappingRequest request, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                return new PutMappingRequest(in);
            }
        }
    }

    private static BytesReference randomMappingSource(XContentType xContentType) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(xContentType)) {
            builder.startObject().startObject("properties").startObject(randomAlphaOfLength(5));
            builder.field("type", "keyword");
            builder.endObject().endObject().endObject();
            return BytesReference.bytes(builder);
        }
    }
}
