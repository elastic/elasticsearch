/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class GetFiltersRequestTests extends AbstractXContentTestCase<GetFiltersRequest> {

    @Override
    protected GetFiltersRequest createTestInstance() {
        GetFiltersRequest request = new GetFiltersRequest();
        if (randomBoolean()) {
            request.setFilterId(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            request.setSize(randomInt(100));
        }
        if (randomBoolean()) {
            request.setFrom(randomInt(100));
        }
        return request;
    }

    @Override
    protected GetFiltersRequest doParseInstance(XContentParser parser) throws IOException {
        return GetFiltersRequest.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
