/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Map;

public class PutMappingRequestTests extends AbstractXContentTestCase<PutMappingRequest> {

    @Override
    protected PutMappingRequest createTestInstance() {
        PutMappingRequest request = new PutMappingRequest();
        if (frequently()) {
            try {
                XContentBuilder builder = RandomCreateIndexGenerator.randomMapping();
                request.source(builder);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return request;
    }

    @Override
    protected PutMappingRequest doParseInstance(XContentParser parser) throws IOException {
        PutMappingRequest request = new PutMappingRequest();
        Map<String, Object> map = parser.map();
        if (map.isEmpty() == false) {
            request.source(map);
        }
        return request;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected void assertEqualInstances(PutMappingRequest expected, PutMappingRequest actual) {
        if (actual.source() != null) {
            try (XContentParser expectedJson = createParser(expected.xContentType().xContent(), expected.source());
                    XContentParser actualJson = createParser(actual.xContentType().xContent(), actual.source())) {
                assertEquals(expectedJson.mapOrdered(), actualJson.mapOrdered());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            // if the original `source` is null, the parsed source should be so too
            assertNull(expected.source());
        }
    }
}
