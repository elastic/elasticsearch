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

import java.util.ArrayList;
import java.util.List;


public class UpdateFilterRequestTests extends AbstractXContentTestCase<UpdateFilterRequest> {

    @Override
    protected UpdateFilterRequest createTestInstance() {
        UpdateFilterRequest request = new UpdateFilterRequest(randomAlphaOfLength(10));
        if (randomBoolean()) {
            request.setDescription(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            int items = randomInt(10);
            List<String> strings = new ArrayList<>(items);
            for (int i = 0; i < items; i++) {
                strings.add(randomAlphaOfLength(10));
            }
            request.setAddItems(strings);
        }
        if (randomBoolean()) {
            int items = randomInt(10);
            List<String> strings = new ArrayList<>(items);
            for (int i = 0; i < items; i++) {
                strings.add(randomAlphaOfLength(10));
            }
            request.setRemoveItems(strings);
        }
        return request;
    }

    @Override
    protected UpdateFilterRequest doParseInstance(XContentParser parser) {
        return UpdateFilterRequest.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
