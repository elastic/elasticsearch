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

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.first.FirstQuery;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;

public class FirstQueryBuilderTests extends AbstractQueryTestCase<FirstQueryBuilder> {

    @Override
    protected FirstQueryBuilder doCreateTestQueryBuilder() {
        int numClauses = randomIntBetween(1, 5);
        List<QueryBuilder> queryBuilders = new ArrayList<>(numClauses);
        for (int i = 0; i < numClauses; i++) {
            queryBuilders.add(RandomQueryBuilder.createQuery(random()));
        }
        FirstQueryBuilder queryBuilder = new FirstQueryBuilder(queryBuilders);
        return queryBuilder;
    }

    @Override
    protected void doAssertLuceneQuery(FirstQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        Collection<Query> queries = AbstractQueryBuilder.toQueries(queryBuilder.getQueryBuilders(), context);
        if (queries.stream().allMatch(q -> q instanceof MatchNoDocsQuery)) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else {
            assertThat(query, instanceOf(FirstQuery.class));
            FirstQuery firstQuery = (FirstQuery) query;
            assertEquals(queryBuilder.getQueryBuilders().size(), firstQuery.getQueries().length);
        }
    }

    public void testFromJson() throws IOException {
        String query =
            "{" +
                "\"first\" : {" +
                "  \"queries\" : [ {" +
                "    \"term\" : {" +
                "      \"tag\" : {" +
                "        \"value\" : \"white\"," +
                "        \"boost\" : 1.0" +
                "      }" +
                "    }" +
                "  }, {" +
                "    \"term\" : {" +
                "      \"tag\" : {" +
                "        \"value\" : \"elephant\"," +
                "        \"boost\" : 1.0" +
                "      }" +
                "    }" +
                "  } ]," +
                "  \"boost\" : 42.0" +
                "}" +
            "}";

        FirstQueryBuilder queryBuilder = (FirstQueryBuilder) parseQuery(query);
        checkGeneratedJson(query, queryBuilder);
        assertEquals(query, 42, queryBuilder.boost, 0.00001);
    }

    public void testRewriteToMatchAll() throws IOException {
        QueryBuilder termQueryBuilder = new TermQueryBuilder("_index", getIndex().getName());
        FirstQueryBuilder firstQueryBuilder = new FirstQueryBuilder(List.of(termQueryBuilder));
        FirstQueryBuilder rewritten = (FirstQueryBuilder) firstQueryBuilder.rewrite(createShardContext());
        assertNotSame(rewritten, firstQueryBuilder);
        assertEquals(new MatchAllQueryBuilder(), rewritten.getQueryBuilders().get(0));
    }

    public void testRewriteToMatchNone() throws IOException {
        QueryShardContext context = createShardContext();
        QueryBuilder termQueryBuilder = new TermsQueryBuilder("unmapped_field", "elephant");
        FirstQueryBuilder firstQueryBuilder = new FirstQueryBuilder(List.of(termQueryBuilder));
        QueryBuilder rewritten = firstQueryBuilder.rewrite(context);
        assertEquals(new MatchNoneQueryBuilder(), rewritten);
    }

    @Override
    public void testMustRewrite() {
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("unmapped_field", "elephant");
        FirstQueryBuilder firstQueryBuilder = new FirstQueryBuilder(List.of(termQueryBuilder));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> firstQueryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }

    public void testFirstQueryBuilderValidation() {
        {
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new FirstQueryBuilder(List.of()));
            assertEquals("[queries] cannot be null or empty!", ex.getMessage());
        }
        {
            FirstQueryBuilder.setMaxClauseCount(6);
            TermQueryBuilder tqb1 = new TermQueryBuilder("field1", "elephant");
            TermQueryBuilder tqb2 = new TermQueryBuilder("field2", "elephant");
            TermQueryBuilder tqb3 = new TermQueryBuilder("field3", "elephant");
            TermQueryBuilder tqb4 = new TermQueryBuilder("field4", "elephant");
            TermQueryBuilder tqb5 = new TermQueryBuilder("field5", "elephant");
            TermQueryBuilder tqb6 = new TermQueryBuilder("field6", "elephant");
            TermQueryBuilder tqb7 = new TermQueryBuilder("field7", "elephant");
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> new FirstQueryBuilder(List.of(tqb1, tqb2, tqb3, tqb4, tqb5, tqb6, tqb7)));
            assertEquals("Too many query clauses! Should be less or equal [6]!", ex.getMessage());
        }
    }
}
