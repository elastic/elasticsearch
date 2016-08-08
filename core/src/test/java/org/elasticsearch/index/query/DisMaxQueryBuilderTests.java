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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class DisMaxQueryBuilderTests extends AbstractQueryTestCase<DisMaxQueryBuilder> {
    /**
     * @return a {@link DisMaxQueryBuilder} with random inner queries
     */
    @Override
    protected DisMaxQueryBuilder doCreateTestQueryBuilder() {
        DisMaxQueryBuilder dismax = new DisMaxQueryBuilder();
        int clauses = randomIntBetween(1, 5);
        for (int i = 0; i < clauses; i++) {
            dismax.add(RandomQueryBuilder.createQuery(random()));
        }
        if (randomBoolean()) {
            dismax.tieBreaker(2.0f / randomIntBetween(1, 20));
        }
        return dismax;
    }

    @Override
    protected void doAssertLuceneQuery(DisMaxQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        Collection<Query> queries = AbstractQueryBuilder.toQueries(queryBuilder.innerQueries(), context);
        assertThat(query, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disjunctionMaxQuery = (DisjunctionMaxQuery) query;
        assertThat(disjunctionMaxQuery.getTieBreakerMultiplier(), equalTo(queryBuilder.tieBreaker()));
        assertThat(disjunctionMaxQuery.getDisjuncts().size(), equalTo(queries.size()));
        Iterator<Query> queryIterator = queries.iterator();
        for (int i = 0; i < disjunctionMaxQuery.getDisjuncts().size(); i++) {
            assertThat(disjunctionMaxQuery.getDisjuncts().get(i), equalTo(queryIterator.next()));
        }
    }

    @Override
    protected Map<String, DisMaxQueryBuilder> getAlternateVersions() {
        Map<String, DisMaxQueryBuilder> alternateVersions = new HashMap<>();
        QueryBuilder innerQuery = createTestQueryBuilder().innerQueries().get(0);
        DisMaxQueryBuilder expectedQuery = new DisMaxQueryBuilder();
        expectedQuery.add(innerQuery);
        String contentString = "{\n" +
                "    \"dis_max\" : {\n" +
                "        \"queries\" : " + innerQuery.toString() +
                "    }\n" +
                "}";
        alternateVersions.put(contentString, expectedQuery);
        return alternateVersions;
    }

    /**
     * Test with empty inner query body, this should be converted to a {@link MatchNoDocsQuery}.
     * To test this, we use inner {@link ConstantScoreQueryBuilder} with empty inner filter.
     */
    public void testInnerQueryEmptyException() throws IOException {
        String queryString = "{ \"" + DisMaxQueryBuilder.NAME + "\" :"
                + "             { \"queries\" : [ {\"" + ConstantScoreQueryBuilder.NAME + "\" : { \"filter\" : { } } } ] "
                + "             }"
                + "           }";
        QueryBuilder queryBuilder = parseQuery(queryString, ParseFieldMatcher.EMPTY);
        QueryShardContext context = createShardContext();
        Query luceneQuery = queryBuilder.toQuery(context);
        assertThat(luceneQuery, instanceOf(MatchNoDocsQuery.class));
        assertThat(((MatchNoDocsQuery) luceneQuery).toString(), equalTo("MatchNoDocsQuery[\"no clauses for dismax query.\"]"));
    }

    public void testIllegalArguments() {
        DisMaxQueryBuilder disMaxQuery = new DisMaxQueryBuilder();
        expectThrows(IllegalArgumentException.class, () -> disMaxQuery.add(null));
    }

    public void testToQueryInnerPrefixQuery() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        String queryAsString = "{\n" +
                "    \"dis_max\":{\n" +
                "        \"queries\":[\n" +
                "            {\n" +
                "                \"prefix\":{\n" +
                "                    \"" + STRING_FIELD_NAME + "\":{\n" +
                "                        \"value\":\"sh\",\n" +
                "                        \"boost\":1.2\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "        ]\n" +
                "    }\n" +
                "}";
        Query query = parseQuery(queryAsString).toQuery(createShardContext());
        assertThat(query, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disjunctionMaxQuery = (DisjunctionMaxQuery) query;

        List<Query> disjuncts = disjunctionMaxQuery.getDisjuncts();
        assertThat(disjuncts.size(), equalTo(1));

        assertThat(disjuncts.get(0), instanceOf(BoostQuery.class));
        BoostQuery boostQuery = (BoostQuery) disjuncts.get(0);
        assertThat((double) boostQuery.getBoost(), closeTo(1.2, 0.00001));
        assertThat(boostQuery.getQuery(), instanceOf(PrefixQuery.class));
        PrefixQuery firstQ = (PrefixQuery) boostQuery.getQuery();
        // since age is automatically registered in data, we encode it as numeric
        assertThat(firstQ.getPrefix(), equalTo(new Term(STRING_FIELD_NAME, "sh")));

    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"dis_max\" : {\n" +
                "    \"tie_breaker\" : 0.7,\n" +
                "    \"queries\" : [ {\n" +
                "      \"term\" : {\n" +
                "        \"age\" : {\n" +
                "          \"value\" : 34,\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    }, {\n" +
                "      \"term\" : {\n" +
                "        \"age\" : {\n" +
                "          \"value\" : 35,\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    } ],\n" +
                "    \"boost\" : 1.2\n" +
                "  }\n" +
                "}";

        DisMaxQueryBuilder parsed = (DisMaxQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 1.2, parsed.boost(), 0.0001);
        assertEquals(json, 0.7, parsed.tieBreaker(), 0.0001);
        assertEquals(json, 2, parsed.innerQueries().size());
    }
}
