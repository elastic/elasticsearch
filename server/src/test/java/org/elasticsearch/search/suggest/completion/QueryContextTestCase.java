/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;


public abstract class QueryContextTestCase<QC extends ToXContent> extends ESTestCase {
    private static final int NUMBER_OF_RUNS = 20;

    /**
     * create random model that is put under test
     */
    protected abstract QC createTestModel();

    /**
     * read the context
     */
    protected abstract QC fromXContent(XContentParser parser) throws IOException;

    public void testToXContext() throws IOException {
        for (int i = 0; i < NUMBER_OF_RUNS; i++) {
            QC toXContent = createTestModel();
            XContentBuilder builder = XContentFactory.jsonBuilder();
            toXContent.toXContent(builder, ToXContent.EMPTY_PARAMS);
            XContentParser parser = createParser(builder);
            parser.nextToken();
            QC fromXContext = fromXContent(parser);
            assertEquals(toXContent, fromXContext);
            assertEquals(toXContent.hashCode(), fromXContext.hashCode());
        }
    }
}
