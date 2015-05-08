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


import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

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
    protected IdsQueryBuilder createEmptyQueryBuilder() {
        return new IdsQueryBuilder();
    }

    @Override
    protected void assertLuceneQuery(IdsQueryBuilder queryBuilder, Query query, QueryParseContext context) throws IOException {
        if (testQuery.ids().size() == 0) {
            assertThat(query, is(instanceOf(BooleanQuery.class)));
        } else {
            assertThat(query, is(instanceOf(TermsQuery.class)));
            TermsQuery termQuery = (TermsQuery) query;
            assertThat(termQuery.getBoost(), is(testQuery.boost()));
            // because internals of TermsQuery are well hidden, check string representation
            String[] parts = termQuery.toString().split(" ");
            assertThat(parts.length, is(queryBuilder.ids().size() * queryBuilder.types().size()));
            assertThat(parts[0].substring(0, parts[0].indexOf(":")), is(UidFieldMapper.NAME));
        }
    }

    public IdsQueryBuilder createTestQueryBuilder() {
        IdsQueryBuilder query = new IdsQueryBuilder();
        int numberOfTypes = randomIntBetween(1, 10);
        String[] types = new String[numberOfTypes];
        for (int i = 0; i < numberOfTypes; i++) {
            types[i] = randomAsciiOfLength(8);
        }
        query = new IdsQueryBuilder(types);
        if (randomBoolean()) {
            int numberOfIds = randomIntBetween(1, 10);
            for (int i = 0; i < numberOfIds; i++) {
                query.addIds(randomAsciiOfLength(8));
            }
        }
        if (randomBoolean()) {
            query.boost(2.0f / randomIntBetween(1, 20));
        }
        return query;
    }
}
