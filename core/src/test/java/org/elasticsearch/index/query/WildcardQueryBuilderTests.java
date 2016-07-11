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
import org.apache.lucene.search.WildcardQuery;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class WildcardQueryBuilderTests extends AbstractQueryTestCase<WildcardQueryBuilder> {

    @Override
    protected WildcardQueryBuilder doCreateTestQueryBuilder() {
        WildcardQueryBuilder query;

        // mapped or unmapped field
        String text = randomAsciiOfLengthBetween(1, 10);
        if (randomBoolean()) {
            query = new WildcardQueryBuilder(STRING_FIELD_NAME, text);
        } else {
            query = new WildcardQueryBuilder(randomAsciiOfLengthBetween(1, 10), text);
        }
        if (randomBoolean()) {
            query.rewrite(randomFrom(getRandomRewriteMethod()));
        }
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(WildcardQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(WildcardQuery.class));
        WildcardQuery wildcardQuery = (WildcardQuery) query;
        assertThat(wildcardQuery.getField(), equalTo(queryBuilder.fieldName()));
        assertThat(wildcardQuery.getTerm().field(), equalTo(queryBuilder.fieldName()));
        assertThat(wildcardQuery.getTerm().text(), equalTo(queryBuilder.value()));
    }

    public void testIllegalArguments() {
        try {
            if (randomBoolean()) {
                new WildcardQueryBuilder(null, "text");
            } else {
                new WildcardQueryBuilder("", "text");
            }
            fail("cannot be null or empty");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            new WildcardQueryBuilder("field", null);
            fail("cannot be null or empty");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    public void testEmptyValue() throws IOException {
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);

        WildcardQueryBuilder wildcardQueryBuilder = new WildcardQueryBuilder(getRandomType(), "");
        assertEquals(wildcardQueryBuilder.toQuery(context).getClass(), WildcardQuery.class);
    }

    public void testFromJson() throws IOException {
        String json =
                "{    \"wildcard\" : { \"user\" : { \"wildcard\" : \"ki*y\", \"boost\" : 2.0 } }}";

        WildcardQueryBuilder parsed = (WildcardQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "ki*y", parsed.value());
        assertEquals(json, 2.0, parsed.boost(), 0.0001);
    }
}
