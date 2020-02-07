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
import org.elasticsearch.index.query.first.FirstQuery;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
        assertThat(query, instanceOf(FirstQuery.class));
        FirstQuery firstQuery = (FirstQuery) query;
        assertEquals(queryBuilder.getQueryBuilders().size(), firstQuery.getQueries().length);
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

    public void testRewrite() throws IOException {
        QueryBuilder queryBuilder = new WrapperQueryBuilder(new TermsQueryBuilder("field", "elephant").toString());
        List<QueryBuilder> queryBuilders = new ArrayList<>(Arrays.asList(queryBuilder));
        FirstQueryBuilder firstQueryBuilder = new FirstQueryBuilder(queryBuilders);
        FirstQueryBuilder rewritten = (FirstQueryBuilder) firstQueryBuilder.rewrite(createShardContext());
        assertNotSame(rewritten, firstQueryBuilder);
        assertEquals(new TermsQueryBuilder("field", "elephant"), rewritten.getQueryBuilders().get(0));
    }
}
