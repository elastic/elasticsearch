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

import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class SpanMultiTermQueryBuilderTests extends AbstractQueryTestCase<SpanMultiTermQueryBuilder> {
    @Override
    protected SpanMultiTermQueryBuilder doCreateTestQueryBuilder() {
        MultiTermQueryBuilder multiTermQueryBuilder = RandomQueryBuilder.createMultiTermQuery(random());
        return new SpanMultiTermQueryBuilder(multiTermQueryBuilder);
    }

    @Override
    protected void doAssertLuceneQuery(SpanMultiTermQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(SpanMultiTermQueryWrapper.class));
        SpanMultiTermQueryWrapper spanMultiTermQueryWrapper = (SpanMultiTermQueryWrapper) query;
        Query multiTermQuery = queryBuilder.innerQuery().toQuery(context);
        assertThat(multiTermQuery, instanceOf(MultiTermQuery.class));
        assertThat(spanMultiTermQueryWrapper.getWrappedQuery(), equalTo(new SpanMultiTermQueryWrapper<>((MultiTermQuery)multiTermQuery).getWrappedQuery()));
    }

    public void testIllegalArgument() {
        try {
            new SpanMultiTermQueryBuilder(null);
            fail("cannot be null");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    /**
     * test checks that we throw an {@link UnsupportedOperationException} if the query wrapped
     * by {@link SpanMultiTermQueryBuilder} does not generate a lucene {@link MultiTermQuery}.
     * This is currently the case for {@link RangeQueryBuilder} when the target field is mapped
     * to a date.
     */
    public void testUnsupportedInnerQueryType() throws IOException {
        QueryShardContext context = createShardContext();
        // test makes only sense if we have at least one type registered with date field mapping
        if (getCurrentTypes().length > 0 && context.fieldMapper(DATE_FIELD_NAME) != null) {
            try {
                RangeQueryBuilder query = new RangeQueryBuilder(DATE_FIELD_NAME);
                new SpanMultiTermQueryBuilder(query).toQuery(createShardContext());
                fail("Exception expected, range query on date fields should not generate a lucene " + MultiTermQuery.class.getName());
            } catch (UnsupportedOperationException e) {
                assert(e.getMessage().contains("unsupported inner query, should be " + MultiTermQuery.class.getName()));
            }
        }
    }
}
