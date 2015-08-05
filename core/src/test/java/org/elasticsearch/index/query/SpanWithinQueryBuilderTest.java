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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanWithinQuery;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;

public class SpanWithinQueryBuilderTest extends BaseQueryTestCase<SpanWithinQueryBuilder> {

    @Override
    protected SpanWithinQueryBuilder doCreateTestQueryBuilder() {
        SpanTermQueryBuilder[] spanTermQueries = new SpanTermQueryBuilderTest().createSpanTermQueryBuilders(2);
        return new SpanWithinQueryBuilder(spanTermQueries[0], spanTermQueries[1]);
    }

    @Override
    protected void doAssertLuceneQuery(SpanWithinQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(SpanWithinQuery.class));
    }

    @Test
    public void testValidate() {
        int totalExpectedErrors = 0;
        SpanQueryBuilder bigSpanQueryBuilder;
        if (randomBoolean()) {
            if (randomBoolean()) {
                bigSpanQueryBuilder = new SpanTermQueryBuilder("", "test");
            } else {
                bigSpanQueryBuilder = null;
            }
            totalExpectedErrors++;
        } else {
            bigSpanQueryBuilder = new SpanTermQueryBuilder("name", "value");
        }
        SpanQueryBuilder littleSpanQueryBuilder;
        if (randomBoolean()) {
            if (randomBoolean()) {
                littleSpanQueryBuilder = new SpanTermQueryBuilder("", "test");
            } else {
                littleSpanQueryBuilder = null;
            }
            totalExpectedErrors++;
        } else {
            littleSpanQueryBuilder = new SpanTermQueryBuilder("name", "value");
        }
        SpanWithinQueryBuilder queryBuilder = new SpanWithinQueryBuilder(bigSpanQueryBuilder, littleSpanQueryBuilder);
        assertValidate(queryBuilder, totalExpectedErrors);
    }
}
