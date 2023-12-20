/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.test.AbstractQueryVectorBuilderTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

/**
 * Test the query vector builder logic with a test plugin
 */
public class QueryVectorBuilderTests extends AbstractQueryVectorBuilderTestCase<TestQueryVectorBuilderPlugin.TestQueryVectorBuilder> {

    @Override
    protected List<SearchPlugin> additionalPlugins() {
        return List.of(new TestQueryVectorBuilderPlugin());
    }

    @Override
    protected Writeable.Reader<TestQueryVectorBuilderPlugin.TestQueryVectorBuilder> instanceReader() {
        return TestQueryVectorBuilderPlugin.TestQueryVectorBuilder::new;
    }

    @Override
    protected TestQueryVectorBuilderPlugin.TestQueryVectorBuilder createTestInstance() {
        return new TestQueryVectorBuilderPlugin.TestQueryVectorBuilder(randomList(2, 1024, ESTestCase::randomFloat));
    }

    @Override
    protected TestQueryVectorBuilderPlugin.TestQueryVectorBuilder createTestInstance(float[] expected) {
        return new TestQueryVectorBuilderPlugin.TestQueryVectorBuilder(expected);
    }

    @Override
    protected TestQueryVectorBuilderPlugin.TestQueryVectorBuilder mutateInstance(
        TestQueryVectorBuilderPlugin.TestQueryVectorBuilder instance
    ) throws IOException {
        return createTestInstance();
    }

    @Override
    protected TestQueryVectorBuilderPlugin.TestQueryVectorBuilder doParseInstance(XContentParser parser) throws IOException {
        return TestQueryVectorBuilderPlugin.TestQueryVectorBuilder.PARSER.apply(parser, null);
    }

    @Override
    protected void doAssertClientRequest(ActionRequest request, TestQueryVectorBuilderPlugin.TestQueryVectorBuilder builder) {
        // Nothing to assert here as this object does not make client calls
    }

    @Override
    protected ActionResponse createResponse(float[] array, TestQueryVectorBuilderPlugin.TestQueryVectorBuilder builder) {
        return new ActionResponse.Empty();
    }
}
