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
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PrefixQueryBuilderTests extends AbstractQueryTestCase<PrefixQueryBuilder> {

    @Override
    protected PrefixQueryBuilder doCreateTestQueryBuilder() {
        PrefixQueryBuilder query = randomPrefixQuery();
        if (randomBoolean()) {
            query.rewrite(getRandomRewriteMethod());
        }
        return query;
    }

    @Override
    protected Map<String, PrefixQueryBuilder> getAlternateVersions() {
        Map<String, PrefixQueryBuilder> alternateVersions = new HashMap<>();
        PrefixQueryBuilder prefixQuery = randomPrefixQuery();
        String contentString = "{\n" +
                "    \"prefix\" : {\n" +
                "        \"" + prefixQuery.fieldName() + "\" : \"" + prefixQuery.value() + "\"\n" +
                "    }\n" +
                "}";
        alternateVersions.put(contentString, prefixQuery);
        return alternateVersions;
    }

    private static PrefixQueryBuilder randomPrefixQuery() {
        String fieldName = randomFrom(TEXT_FIELD_NAME,
            TEXT_ALIAS_FIELD_NAME,
            randomAlphaOfLengthBetween(1, 10));
        String value = randomAlphaOfLengthBetween(1, 10);
        return new PrefixQueryBuilder(fieldName, value);
    }

    @Override
    protected void doAssertLuceneQuery(PrefixQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, Matchers.anyOf(instanceOf(PrefixQuery.class), instanceOf(MatchNoDocsQuery.class)));
        if (context.fieldMapper(queryBuilder.fieldName()) != null) { // The field is mapped
            PrefixQuery prefixQuery = (PrefixQuery) query;

            String expectedFieldName = expectedFieldName(queryBuilder.fieldName());
            assertThat(prefixQuery.getPrefix().field(), equalTo(expectedFieldName));
            assertThat(prefixQuery.getPrefix().text(), equalTo(queryBuilder.value()));
        }
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new PrefixQueryBuilder(null, "text"));
        assertEquals("field name is null or empty", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new PrefixQueryBuilder("", "text"));
        assertEquals("field name is null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new PrefixQueryBuilder("field", null));
        assertEquals("value cannot be null", e.getMessage());
    }

    public void testBlendedRewriteMethod() throws IOException {
        String rewrite = "top_terms_blended_freqs_10";
        Query parsedQuery = parseQuery(prefixQuery(TEXT_FIELD_NAME, "val").rewrite(rewrite)).toQuery(createShardContext());
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        assertThat(prefixQuery.getPrefix(), equalTo(new Term(TEXT_FIELD_NAME, "val")));
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
        PrefixQueryBuilder query = prefixQuery(INT_FIELD_NAME, "12*");
        QueryShardContext context = createShardContext();
        QueryShardException e = expectThrows(QueryShardException.class,
                () -> query.toQuery(context));
        assertEquals("Can only use prefix queries on keyword, text and wildcard fields - not on [mapped_int] which is of type [integer]",
                e.getMessage());
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json =
                "{\n" +
                "    \"prefix\": {\n" +
                "      \"user1\": {\n" +
                "        \"value\": \"ki\"\n" +
                "      },\n" +
                "      \"user2\": {\n" +
                "        \"value\": \"ki\"\n" +
                "      }\n" +
                "    }\n" +
                "}";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[prefix] query doesn't support multiple fields, found [user1] and [user2]", e.getMessage());

        String shortJson =
                "{\n" +
                "    \"prefix\": {\n" +
                "      \"user1\": \"ki\",\n" +
                "      \"user2\": \"ki\"\n" +
                "    }\n" +
                "}";
        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[prefix] query doesn't support multiple fields, found [user1] and [user2]", e.getMessage());
    }
    
    public void testRewriteIndexQueryToMatchNone() throws Exception {
        PrefixQueryBuilder query = prefixQuery("_index", "does_not_exist");
        QueryShardContext queryShardContext = createShardContext();
        QueryBuilder rewritten = query.rewrite(queryShardContext);
        assertThat(rewritten, instanceOf(MatchNoneQueryBuilder.class));
    }

    public void testRewriteIndexQueryToNotMatchNone() throws Exception {
        PrefixQueryBuilder query = prefixQuery("_index", getIndex().getName());
        QueryShardContext queryShardContext = createShardContext();
        QueryBuilder rewritten = query.rewrite(queryShardContext);
        assertThat(rewritten, instanceOf(MatchAllQueryBuilder.class));
    }

    @Override
    public void testMustRewrite() throws IOException {
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);
        PrefixQueryBuilder queryBuilder = new PrefixQueryBuilder("unmapped_field", "foo");
        IllegalStateException e = expectThrows(IllegalStateException.class,
                () -> queryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }
}
