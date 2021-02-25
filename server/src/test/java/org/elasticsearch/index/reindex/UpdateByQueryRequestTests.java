/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.apache.lucene.util.TestUtil.randomSimpleString;

public class UpdateByQueryRequestTests extends AbstractBulkByScrollRequestTestCase<UpdateByQueryRequest> {
    public void testUpdateByQueryRequestImplementsIndicesRequestReplaceable() {
        int numIndices = between(1, 100);
        String[] indices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indices[i] = randomSimpleString(random(), 1, 30);
        }

        IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());

        UpdateByQueryRequest request = new UpdateByQueryRequest();
        request.indices(indices);
        request.setIndicesOptions(indicesOptions);
        for (int i = 0; i < numIndices; i++) {
            assertEquals(indices[i], request.indices()[i]);
        }

        assertSame(indicesOptions, request.indicesOptions());
        assertSame(request.indicesOptions(), request.getSearchRequest().indicesOptions());

        int numNewIndices = between(1, 100);
        String[] newIndices = new String[numNewIndices];
        for (int i = 0; i < numNewIndices; i++) {
            newIndices[i] = randomSimpleString(random(), 1, 30);
        }
        request.indices(newIndices);
        for (int i = 0; i < numNewIndices; i++) {
            assertEquals(newIndices[i], request.indices()[i]);
        }
        for (int i = 0; i < numNewIndices; i++) {
            assertEquals(newIndices[i], request.getSearchRequest().indices()[i]);
        }
    }

    @Override
    protected UpdateByQueryRequest newRequest() {
        return new UpdateByQueryRequest(randomAlphaOfLength(5));
    }

    @Override
    protected void extraRandomizationForSlice(UpdateByQueryRequest original) {
        if (randomBoolean()) {
            original.setScript(mockScript(randomAlphaOfLength(5)));
        }
        if (randomBoolean()) {
            original.setPipeline(randomAlphaOfLength(5));
        }
    }

    @Override
    protected void extraForSliceAssertions(UpdateByQueryRequest original, UpdateByQueryRequest forSliced) {
        assertEquals(original.getScript(), forSliced.getScript());
        assertEquals(original.getPipeline(), forSliced.getPipeline());
    }

    // TODO: Implement standard to/from x-content parsing tests

    @Override
    protected UpdateByQueryRequest createTestInstance() {
        return newRequest();
    }

    @Override
    protected UpdateByQueryRequest doParseInstance(XContentParser parser) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != null) {
        }
        return newRequest();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected void assertEqualInstances(UpdateByQueryRequest expectedInstance, UpdateByQueryRequest newInstance) {
    }
}
