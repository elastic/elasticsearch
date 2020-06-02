/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchbusinessrules;

import com.fasterxml.jackson.core.io.JsonStringEncoder;

import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;

public class PinnedQueryBuilderTests extends AbstractQueryTestCase<PinnedQueryBuilder> {
    @Override
    protected PinnedQueryBuilder doCreateTestQueryBuilder() {
        return new PinnedQueryBuilder(createRandomQuery(), generateRandomStringArray(100, 256, false, true));
    }

    private QueryBuilder createRandomQuery() {
        if (randomBoolean()) {
            return new MatchAllQueryBuilder();
        } else {
            return createTestTermQueryBuilder();
        }
    }

    private QueryBuilder createTestTermQueryBuilder() {
            String fieldName = null;
            Object value;
            switch (randomIntBetween(0, 3)) {
                case 0:
                    if (randomBoolean()) {
                        fieldName = BOOLEAN_FIELD_NAME;
                    }
                    value = randomBoolean();
                    break;
                case 1:
                    if (randomBoolean()) {
                        fieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME);
                    }
                    if (frequently()) {
                        value = randomAlphaOfLengthBetween(1, 10);
                    } else {
                        // generate unicode string in 10% of cases
                        JsonStringEncoder encoder = JsonStringEncoder.getInstance();
                        value = new String(encoder.quoteAsString(randomUnicodeOfLength(10)));
                    }
                    break;
                case 2:
                    if (randomBoolean()) {
                        fieldName = INT_FIELD_NAME;
                    }
                    value = randomInt(10000);
                    break;
                case 3:
                    if (randomBoolean()) {
                        fieldName = DOUBLE_FIELD_NAME;
                    }
                    value = randomDouble();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }

            if (fieldName == null) {
                fieldName = randomAlphaOfLengthBetween(1, 10);
            }
            return new TermQueryBuilder(fieldName, value);
        }

    @Override
    protected void doAssertLuceneQuery(PinnedQueryBuilder queryBuilder, Query query, QueryShardContext searchContext) throws IOException {
        if (queryBuilder.ids().size() == 0 && queryBuilder.organicQuery() == null) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else {
            if (queryBuilder.ids().size() > 0) {
                // Have IDs and an organic query - uses DisMax
                assertThat(query, instanceOf(DisjunctionMaxQuery.class));
            }
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> classpathPlugins = new ArrayList<>();
        classpathPlugins.add(SearchBusinessRules.class);
        classpathPlugins.add(TestGeoShapeFieldMapperPlugin.class);
        return classpathPlugins;
    }

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new PinnedQueryBuilder(new MatchAllQueryBuilder(), (String)null));
        expectThrows(IllegalArgumentException.class, () -> new PinnedQueryBuilder(null, "1"));
        expectThrows(IllegalArgumentException.class, () -> new PinnedQueryBuilder(new MatchAllQueryBuilder(), "1", null, "2"));
        String[] bigList = new String[PinnedQueryBuilder.MAX_NUM_PINNED_HITS + 1];
        for (int i = 0; i < bigList.length; i++) {
            bigList[i] = String.valueOf(i);
        }
        expectThrows(IllegalArgumentException.class, () -> new PinnedQueryBuilder(new MatchAllQueryBuilder(), bigList));
        
    }

    public void testEmptyPinnedQuery() throws Exception {
        XContentBuilder contentBuilder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        contentBuilder.startObject().startObject("pinned").endObject().endObject();
        try (XContentParser xParser = createParser(contentBuilder)) {
            expectThrows(ParsingException.class, () -> parseQuery(xParser).toQuery(createShardContext()));
        }
    }

    public void testFromJson() throws IOException {
        String query =
                "{" +
                "\"pinned\" : {" +
                "  \"organic\" : {" +
                "    \"term\" : {" +
                "      \"tag\" : {" +
                "        \"value\" : \"tech\"," +
                "        \"boost\" : 1.0" +
                "      }" +
                "    }" +
                "  }, "+
                "  \"ids\" : [ \"1\",\"2\" ]," +
                "  \"boost\":1.0 "+
                "}" +
              "}";

        PinnedQueryBuilder queryBuilder = (PinnedQueryBuilder) parseQuery(query);
        checkGeneratedJson(query, queryBuilder);

        assertEquals(query, 2, queryBuilder.ids().size());
        assertThat(queryBuilder.organicQuery(), instanceOf(TermQueryBuilder.class));
    }

    /**
     * test that unknown query names in the clauses throw an error
     */
    public void testUnknownQueryName() throws IOException {
        String query = "{\"pinned\" : {\"organic\" : { \"unknown_query\" : { } } } }";

        ParsingException ex = expectThrows(ParsingException.class, () -> parseQuery(query));
        // BoolQueryBuilder test has this test for a more detailed error message:
        // assertEquals("no [query] registered for [unknown_query]", ex.getMessage());
        // But ObjectParser used in PinnedQueryBuilder tends to hide the above message and give this below:
        assertEquals("[1:46] [pinned] failed to parse field [organic]", ex.getMessage());
    }

    public void testRewrite() throws IOException {
        PinnedQueryBuilder pinnedQueryBuilder = new PinnedQueryBuilder(new TermQueryBuilder("foo", 1), "1");
        QueryBuilder rewritten = pinnedQueryBuilder.rewrite(createShardContext());
        assertThat(rewritten, instanceOf(PinnedQueryBuilder.class));
    }

    @Override
    public void testMustRewrite() throws IOException {
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);
        PinnedQueryBuilder queryBuilder = new PinnedQueryBuilder(new TermQueryBuilder("unmapped_field", "42"));
        IllegalStateException e = expectThrows(IllegalStateException.class,
                () -> queryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }
}
