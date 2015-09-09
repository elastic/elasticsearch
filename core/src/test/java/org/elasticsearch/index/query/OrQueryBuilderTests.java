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
import org.apache.lucene.search.Query;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.CoreMatchers.*;

@SuppressWarnings("deprecation")
public class OrQueryBuilderTests extends AbstractQueryTestCase<OrQueryBuilder> {

    /**
     * @return an OrQueryBuilder with random limit between 0 and 20
     */
    @Override
    protected OrQueryBuilder doCreateTestQueryBuilder() {
        OrQueryBuilder query = new OrQueryBuilder();
        int subQueries = randomIntBetween(1, 5);
        for (int i = 0; i < subQueries; i++ ) {
            query.add(RandomQueryBuilder.createQuery(random()));
        }
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(OrQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        if (queryBuilder.innerQueries().isEmpty()) {
            assertThat(query, nullValue());
        } else {
            List<Query> innerQueries = new ArrayList<>();
            for (QueryBuilder subQuery : queryBuilder.innerQueries()) {
                Query innerQuery = subQuery.toQuery(context);
                // ignore queries that are null
                if (innerQuery != null) {
                    innerQueries.add(innerQuery);
                }
            }
            if (innerQueries.isEmpty()) {
                assertThat(query, nullValue());
            } else {
                assertThat(query, instanceOf(BooleanQuery.class));
                BooleanQuery booleanQuery = (BooleanQuery) query;
                assertThat(booleanQuery.clauses().size(), equalTo(innerQueries.size()));
                Iterator<Query> queryIterator = innerQueries.iterator();
                for (BooleanClause booleanClause : booleanQuery) {
                    assertThat(booleanClause.getOccur(), equalTo(BooleanClause.Occur.SHOULD));
                    assertThat(booleanClause.getQuery(), equalTo(queryIterator.next()));
                }
            }
        }
    }

    /**
     * test corner case where no inner queries exist
     */
    @Test
    public void testNoInnerQueries() throws QueryShardException, IOException {
        OrQueryBuilder orQuery = new OrQueryBuilder();
        assertNull(orQuery.toQuery(createShardContext()));
    }

    @Override
    protected Map<String, OrQueryBuilder> getAlternateVersions() {
        Map<String, OrQueryBuilder> alternateVersions = new HashMap<>();
        QueryBuilder innerQuery = createTestQueryBuilder().innerQueries().get(0);
        OrQueryBuilder expectedQuery = new OrQueryBuilder(innerQuery);
        String contentString =  "{ \"or\" : [ " + innerQuery + "] }";
        alternateVersions.put(contentString, expectedQuery);
        return alternateVersions;
    }

    @Test(expected=QueryParsingException.class)
    public void testMissingFiltersSection() throws IOException {
        String queryString = "{ \"or\" : {}";
        parseQuery(queryString);
    }

    @Test
    public void testValidate() {
        OrQueryBuilder orQuery = new OrQueryBuilder();
        int iters = randomIntBetween(0, 5);
        int totalExpectedErrors = 0;
        for (int i = 0; i < iters; i++) {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    orQuery.add(RandomQueryBuilder.createInvalidQuery(random()));
                } else {
                    orQuery.add(null);
                }
                totalExpectedErrors++;
            } else {
                orQuery.add(RandomQueryBuilder.createQuery(random()));
            }
        }
        assertValidate(orQuery, totalExpectedErrors);
    }
}
