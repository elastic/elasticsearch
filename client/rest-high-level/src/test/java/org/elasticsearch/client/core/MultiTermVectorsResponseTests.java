/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.core;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class MultiTermVectorsResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(
            this::createParser,
            this::createTestInstance,
            this::toXContent,
            MultiTermVectorsResponse::fromXContent)
            .supportsUnknownFields(true)
            .randomFieldsExcludeFilter(field ->
                field.endsWith("term_vectors") || field.endsWith("terms") || field.endsWith("tokens"))
            .test();
    }

    private void toXContent(MultiTermVectorsResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        List<TermVectorsResponse> termVectorsResponseList = response.getTermVectorsResponses();
        if (termVectorsResponseList != null) {
            builder.startArray("docs");
            for (TermVectorsResponse tvr : termVectorsResponseList) {
                TermVectorsResponseTests.toXContent(tvr, builder);
            }
            builder.endArray();
        }
        builder.endObject();
    }

    protected MultiTermVectorsResponse createTestInstance() {
        int numberOfResponses = randomIntBetween(0, 5);
        List<TermVectorsResponse> responses = new ArrayList<>(numberOfResponses);
        for (int i = 0; i < numberOfResponses; i++) {
            TermVectorsResponse tvResponse = TermVectorsResponseTests.createTestInstance();
            responses.add(tvResponse);
        }
        return new MultiTermVectorsResponse(responses);
    }
}
