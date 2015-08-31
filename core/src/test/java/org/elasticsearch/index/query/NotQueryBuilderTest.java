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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.*;

public class NotQueryBuilderTest extends BaseQueryTestCase<NotQueryBuilder> {

    /**
     * @return a NotQueryBuilder with random limit between 0 and 20
     */
    @Override
    protected NotQueryBuilder doCreateTestQueryBuilder() {
        return new NotQueryBuilder(RandomQueryBuilder.createQuery(random()));
    }

    @Override
    protected void doAssertLuceneQuery(NotQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        Query filter = queryBuilder.innerQuery().toQuery(context);
        if (filter == null) {
            assertThat(query, nullValue());
        } else {
            assertThat(query, instanceOf(BooleanQuery.class));
            BooleanQuery booleanQuery = (BooleanQuery) query;
            assertThat(booleanQuery.clauses().size(), equalTo(2));
            assertThat(booleanQuery.clauses().get(0).getOccur(), equalTo(BooleanClause.Occur.MUST));
            assertThat(booleanQuery.clauses().get(0).getQuery(), instanceOf(MatchAllDocsQuery.class));
            assertThat(booleanQuery.clauses().get(1).getOccur(), equalTo(BooleanClause.Occur.MUST_NOT));
            assertThat(booleanQuery.clauses().get(1).getQuery(), equalTo(filter));
        }
    }

    /**
     * @throws IOException
     */
    @Test(expected=QueryParsingException.class)
    public void testMissingFilterSection() throws IOException {
        String queryString = "{ \"not\" : {}";
        parseQuery(queryString);
    }

    @Override
    protected Map<String, NotQueryBuilder> getAlternateVersions() {
        Map<String, NotQueryBuilder> alternateVersions = new HashMap<>();

        NotQueryBuilder testQuery1 = new NotQueryBuilder(createTestQueryBuilder().innerQuery());
        String contentString1 = "{\n" +
                "    \"not\" : {\n" +
                "        \"filter\" : " + testQuery1.innerQuery().toString() + "\n" +
                "    }\n" +
                "}";
        alternateVersions.put(contentString1, testQuery1);

        QueryBuilder innerQuery = createTestQueryBuilder().innerQuery();
        //not doesn't support empty query when query/filter element is not specified
        if (innerQuery != EmptyQueryBuilder.PROTOTYPE) {
            NotQueryBuilder testQuery2 = new NotQueryBuilder(innerQuery);
            String contentString2 = "{\n" +
                    "    \"not\" : " + testQuery2.innerQuery().toString() +  "\n}";
            alternateVersions.put(contentString2, testQuery2);
        }

        return alternateVersions;
    }

    @Test
    public void testValidate() {
        QueryBuilder innerQuery = null;
        int totalExpectedErrors = 0;
        if (randomBoolean()) {
            if (randomBoolean()) {
                innerQuery = RandomQueryBuilder.createInvalidQuery(random());
            }
            totalExpectedErrors++;
        } else {
            innerQuery = RandomQueryBuilder.createQuery(random());
        }
        NotQueryBuilder notQuery = new NotQueryBuilder(innerQuery);
        assertValidate(notQuery, totalExpectedErrors);
    }
}
