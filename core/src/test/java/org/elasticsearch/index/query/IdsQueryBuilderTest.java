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


import com.google.common.collect.Sets;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;

public class IdsQueryBuilderTest extends BaseQueryTestCase<IdsQueryBuilder> {

    /**
     * check that parser throws exception on missing values field
     * @throws IOException
     */
    @Test(expected=QueryParsingException.class)
    public void testIdsNotProvided() throws IOException {
        String noIdsFieldQuery = "{\"ids\" : { \"type\" : \"my_type\"  }";
        XContentParser parser = XContentFactory.xContent(noIdsFieldQuery).createParser(noIdsFieldQuery);
        QueryParseContext context = createContext();
        context.reset(parser);
        assertQueryHeader(parser, "ids");
        context.indexQueryParserService().queryParser("ids").fromXContent(context);
    }

    @Override
    protected Query createExpectedQuery(IdsQueryBuilder queryBuilder, QueryParseContext context) throws IOException {
        Query expectedQuery;
        if (queryBuilder.ids().size() == 0) {
            expectedQuery = Queries.newMatchNoDocsQuery();
        } else {
            String[] typesForQuery;
            if (queryBuilder.types() == null || queryBuilder.types().length == 0) {
                Collection<String> queryTypes = context.queryTypes();
                typesForQuery = queryTypes.toArray(new String[queryTypes.size()]);
            } else if (queryBuilder.types().length == 1 && MetaData.ALL.equals(queryBuilder.types()[0])) {
                typesForQuery = getCurrentTypes();
            } else {
                typesForQuery = queryBuilder.types();
            }
            expectedQuery = new TermsQuery(UidFieldMapper.NAME, Uid.createUidsForTypesAndIds(Sets.newHashSet(typesForQuery), queryBuilder.ids()));
        }
        expectedQuery.setBoost(queryBuilder.boost());
        return expectedQuery;
    }

    @Override
    protected void assertLuceneQuery(IdsQueryBuilder queryBuilder, Query query, QueryParseContext context) {
        if (queryBuilder.queryName() != null) {
            Query namedQuery = context.copyNamedFilters().get(queryBuilder.queryName());
            assertThat(namedQuery, equalTo(query));
        }
    }

    @Override
    protected RandomQueryBuilder<IdsQueryBuilder> getRandomQueryBuilder() {
        return new RandomIdsQueryBuilder();
    }
}
