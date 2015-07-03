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

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

import java.io.IOException;

@SuppressWarnings("deprecation")
public class OrQueryBuilderTest extends BaseQueryTestCase<OrQueryBuilder> {

    @Override
    protected Query doCreateExpectedQuery(OrQueryBuilder queryBuilder, QueryParseContext context) throws QueryParsingException, IOException {
        if (queryBuilder.filters().isEmpty()) {
            return null;
        }
        BooleanQuery query = new BooleanQuery();
        for (QueryBuilder subQuery : queryBuilder.filters()) {
            Query innerQuery = subQuery.toQuery(context);
            // ignore queries that are null
            if (innerQuery != null) {
                query.add(innerQuery, Occur.SHOULD);
            }
        }
        if (query.clauses().isEmpty()) {
            return null;
        }
        return query;
    }

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

    /**
     * test corner case where no inner queries exist
     */
    @Test
    public void testNoInnerQueries() throws QueryParsingException, IOException {
        OrQueryBuilder orQuery = new OrQueryBuilder();
        assertNull(orQuery.toQuery(createContext()));
    }

    @Test(expected=QueryParsingException.class)
    public void testMissingFiltersSection() throws IOException {
        QueryParseContext context = createContext();
        String queryString = "{ \"or\" : {}";
        XContentParser parser = XContentFactory.xContent(queryString).createParser(queryString);
        context.reset(parser);
        assertQueryHeader(parser, OrQueryBuilder.PROTOTYPE.getName());
        context.indexQueryParserService().queryParser(OrQueryBuilder.PROTOTYPE.getName()).fromXContent(context);
    }

    @Test
    public void testValidate() {
        OrQueryBuilder orQuery = new OrQueryBuilder();
        int iters = randomIntBetween(0, 5);
        int totalExpectedErrors = 0;
        for (int i = 0; i < iters; i++) {
            if (randomBoolean()) {
                orQuery.add(RandomQueryBuilder.createInvalidQuery(random()));
                totalExpectedErrors++;
            } else {
                orQuery.add(RandomQueryBuilder.createQuery(random()));
            }
        }
        assertValidate(orQuery, totalExpectedErrors);
    }

    @Test(expected=NullPointerException.class)
    public void testNullConstructor() {
        new OrQueryBuilder(EmptyQueryBuilder.PROTOTYPE, null);
    }

    @Test(expected=NullPointerException.class)
    public void testAddNull() {
        new OrQueryBuilder().add(null);
    }
}
