/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
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
    protected abstract QC fromXContent(QueryParseContext context) throws IOException;

    public void testToXContext() throws IOException {
        for (int i = 0; i < NUMBER_OF_RUNS; i++) {
            QC toXContent = createTestModel();
            XContentBuilder builder = XContentFactory.jsonBuilder();
            toXContent.toXContent(builder, ToXContent.EMPTY_PARAMS);
            BytesReference bytesReference = builder.bytes();
            XContentParser parser = XContentFactory.xContent(bytesReference).createParser(bytesReference);
            parser.nextToken();
            QC fromXContext = fromXContent(new QueryParseContext(new IndicesQueriesRegistry(), parser, ParseFieldMatcher.STRICT));
            assertEquals(toXContent, fromXContext);
            assertEquals(toXContent.hashCode(), fromXContext.hashCode());
        }
    }
}
