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
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PrefixQueryBuilderTests extends AbstractQueryTestCase<PrefixQueryBuilder> {

    @Override
    protected PrefixQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomBoolean() ? STRING_FIELD_NAME : randomAsciiOfLengthBetween(1, 10);
        String value = randomAsciiOfLengthBetween(1, 10);
        PrefixQueryBuilder query = new PrefixQueryBuilder(fieldName, value);

        if (randomBoolean()) {
            query.rewrite(getRandomRewriteMethod());
        }
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(PrefixQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) query;
        assertThat(prefixQuery.getPrefix().field(), equalTo(queryBuilder.fieldName()));
        assertThat(prefixQuery.getPrefix().text(), equalTo(queryBuilder.value()));
    }

    public void testIllegalArguments() {
        try {
            if (randomBoolean()) {
                new PrefixQueryBuilder(null, "text");
            } else {
                new PrefixQueryBuilder("", "text");
            }
            fail("cannot be null or empty");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            new PrefixQueryBuilder("field", null);
            fail("cannot be null or empty");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    public void testBlendedRewriteMethod() throws IOException {
        String rewrite = "top_terms_blended_freqs_10";
        Query parsedQuery = parseQuery(prefixQuery("field", "val").rewrite(rewrite).buildAsBytes()).toQuery(createShardContext());
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        assertThat(prefixQuery.getPrefix(), equalTo(new Term("field", "val")));
        assertThat(prefixQuery.getRewriteMethod(), instanceOf(MultiTermQuery.TopTermsBlendedFreqScoringRewrite.class));
    }

    public void testFromJson() throws IOException {
        String json =
                "{    \"prefix\" : { \"user\" :  { \"value\" : \"ki\", \"boost\" : 2.0 } }}";

        PrefixQueryBuilder parsed = (PrefixQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "ki", parsed.value());
        assertEquals(json, 2.0, parsed.boost(), 0.00001);
        assertEquals(json, "user", parsed.fieldName());
    }

    public void testNumeric() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        PrefixQueryBuilder query = prefixQuery(INT_FIELD_NAME, "12*");
        QueryShardContext context = createShardContext();
        QueryShardException e = expectThrows(QueryShardException.class,
                () -> query.toQuery(context));
        assertEquals("Can only use prefix queries on keyword and text fields - not on [mapped_int] which is of type [integer]",
                e.getMessage());
    }
}
