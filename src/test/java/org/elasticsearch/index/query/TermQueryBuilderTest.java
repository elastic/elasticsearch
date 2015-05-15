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
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.lucene.BytesRefs;

import java.io.IOException;

import static org.hamcrest.Matchers.*;

public class TermQueryBuilderTest extends BaseTermQueryTestCase<TermQueryBuilder> {

    @Override
    protected TermQueryBuilder createEmptyQueryBuilder() {
        return new TermQueryBuilder(null, null);
    }

    /**
     * @return a TermQuery with random field name and value, optional random boost and queryname
     */
    @Override
    protected TermQueryBuilder createQueryBuilder(String fieldName, Object value) {
        return new TermQueryBuilder(fieldName, value);
    }

    /** Returns a TermQuery with random field name and value, optional random boost and queryname. */
    @Override
    protected TermQueryBuilder createTestQueryBuilder() {
        Object value = createRandomValueObject();
        TermQueryBuilder query = new TermQueryBuilder(randomAsciiOfLength(8), value);
        if (randomBoolean()) {
            query.boost(2.0f / randomIntBetween(1, 20));
        }
        if (randomBoolean()) {
            query.queryName(randomAsciiOfLengthBetween(1, 10));
        }
        return query;
    }

    /** Validates the Lucene query that was generated from a given {@link TermQueryBuilder}*/
    @Override
    protected void assertLuceneQuery(TermQueryBuilder queryBuilder, Query query, QueryParseContext context) throws IOException {
        assertThat(query, instanceOf(TermQuery.class));
        assertThat(query.getBoost(), is(queryBuilder.boost()));
        TermQuery termQuery = (TermQuery) query;
        assertThat(termQuery.getTerm().field(), is(queryBuilder.fieldName()));
        assertThat(termQuery.getTerm().bytes(), is(BytesRefs.toBytesRef(queryBuilder.value())));
        if (queryBuilder.queryName() != null) {
            Query namedQuery = context.copyNamedFilters().get(queryBuilder.queryName());
            assertThat(namedQuery, equalTo((Query)termQuery));
        }
    }
}
