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
import org.apache.lucene.search.spans.SpanTermQuery;
import org.elasticsearch.common.lucene.BytesRefs;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SpanTermQueryBuilderTest extends BaseTermQueryTestCase<SpanTermQueryBuilder> {

    @Override
    protected SpanTermQueryBuilder createEmptyQueryBuilder() {
        return new SpanTermQueryBuilder();
    }
    
    @Override
    protected SpanTermQueryBuilder createQueryBuilder(String fieldName, Object value) {
        return new SpanTermQueryBuilder(fieldName, value);
    }

    /** Returns a {@link SpanTermQueryBuilder} with random field name and value, optional random boost and queryname */
    @Override
    protected SpanTermQueryBuilder createTestQueryBuilder() {
        Object value = createRandomValueObject();
        SpanTermQueryBuilder query = new SpanTermQueryBuilder(randomAsciiOfLength(8), value);
        if (randomBoolean()) {
            query.boost(2.0f / randomIntBetween(1, 20));
        }
        if (randomBoolean()) {
            query.queryName(randomAsciiOfLength(8));
        }
        return query;
    }

    /** Checks the generated Lucene query against the {@link SpanTermQueryBuilder} it was created from. */
    @Override
    protected void assertLuceneQuery(SpanTermQueryBuilder queryBuilder, Query query, QueryParseContext context) throws IOException {
        assertThat(query, instanceOf(SpanTermQuery.class));
        assertThat(query.getBoost(), is(queryBuilder.boost()));
        SpanTermQuery termQuery = (SpanTermQuery) query;
        assertThat(termQuery.getTerm().field(), is(queryBuilder.fieldName()));
        assertThat(termQuery.getTerm().bytes(), is(BytesRefs.toBytesRef(queryBuilder.value())));
        if (queryBuilder.queryName() != null) {
            Query namedQuery = context.copyNamedFilters().get(queryBuilder.queryName());
            assertThat(namedQuery, equalTo((Query)termQuery));
        }
    }
}
