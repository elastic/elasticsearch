/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules;

import com.fasterxml.jackson.core.io.JsonStringEncoder;

import org.apache.lucene.search.CappedScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;
import org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder.Item;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;

public class PinnedQueryBuilderTests extends AbstractQueryTestCase<PinnedQueryBuilder> {
    @Override
    protected PinnedQueryBuilder doCreateTestQueryBuilder() {
        if (randomBoolean()) {
            return new PinnedQueryBuilder(createRandomQuery(), generateRandomStringArray(100, 256, false, true));
        } else {
            return new PinnedQueryBuilder(createRandomQuery(), generateRandomItems());
        }
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

    private Item[] generateRandomItems() {
        return randomArray(1, 100, Item[]::new, () -> new Item(randomAlphaOfLength(64), randomAlphaOfLength(256)));
    }

    @Override
    protected void doAssertLuceneQuery(PinnedQueryBuilder queryBuilder, Query query, SearchExecutionContext searchContext) {
        if (queryBuilder.ids().size() == 0 && queryBuilder.docs().size() == 0) {
            assertThat(query, instanceOf(CappedScoreQuery.class));
        } else {
            // Have IDs/docs and an organic query - uses DisMax
            assertThat(query, instanceOf(DisjunctionMaxQuery.class));
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
        expectThrows(
            IllegalArgumentException.class,
            () -> new PinnedQueryBuilder(new MatchAllQueryBuilder(), (PinnedQueryBuilder.Item)null)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new PinnedQueryBuilder(null, new Item("test", "1"))
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new PinnedQueryBuilder(new MatchAllQueryBuilder(), new Item("test", "1"), null, new Item("test", "2"))
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new PinnedQueryBuilder(new MatchAllQueryBuilder(), new Item("test*", "1"))
        );
        String[] bigIdList = new String[PinnedQueryBuilder.MAX_NUM_PINNED_HITS + 1];
        Item[] bigItemList = new Item[PinnedQueryBuilder.MAX_NUM_PINNED_HITS + 1];
        for (int i = 0; i < bigIdList.length; i++) {
            bigIdList[i] = String.valueOf(i);
            bigItemList[i] = new Item("test", String.valueOf(i));
        }
        expectThrows(IllegalArgumentException.class, () -> new PinnedQueryBuilder(new MatchAllQueryBuilder(), bigIdList));
        expectThrows(IllegalArgumentException.class, () -> new PinnedQueryBuilder(new MatchAllQueryBuilder(), bigItemList));

    }

    public void testEmptyPinnedQuery() throws Exception {
        XContentBuilder contentBuilder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        contentBuilder.startObject().startObject("pinned").endObject().endObject();
        try (XContentParser xParser = createParser(contentBuilder)) {
            expectThrows(ParsingException.class, () -> parseQuery(xParser).toQuery(createSearchExecutionContext()));
        }
    }

    public void testIdsFromJson() throws IOException {
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

    public void testDocsFromJson() throws IOException {
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
                "  \"docs\" : [{ \"_index\": \"test\", \"_id\": \"1\" }, { \"_index\": \"test\", \"_id\": \"2\" }]," +
                "  \"boost\":1.0 "+
                "}" +
              "}";

        PinnedQueryBuilder queryBuilder = (PinnedQueryBuilder) parseQuery(query);
        checkGeneratedJson(query, queryBuilder);

        assertEquals(query, 2, queryBuilder.docs().size());
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

    public void testIdsRewrite() throws IOException {
        PinnedQueryBuilder pinnedQueryBuilder = new PinnedQueryBuilder(new TermQueryBuilder("foo", 1), "1");
        QueryBuilder rewritten = pinnedQueryBuilder.rewrite(createSearchExecutionContext());
        assertThat(rewritten, instanceOf(PinnedQueryBuilder.class));
    }

    public void testDocsRewrite() throws IOException {
        PinnedQueryBuilder pinnedQueryBuilder = new PinnedQueryBuilder(new TermQueryBuilder("foo", 1), new Item("test", "1"));
        QueryBuilder rewritten = pinnedQueryBuilder.rewrite(createSearchExecutionContext());
        assertThat(rewritten, instanceOf(PinnedQueryBuilder.class));
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        PinnedQueryBuilder queryBuilder = new PinnedQueryBuilder(new TermQueryBuilder("unmapped_field", "42"), "42");
        IllegalStateException e = expectThrows(IllegalStateException.class,
                () -> queryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }

    public void testIdInsertionOrderRetained() {
        String[] ids = generateRandomStringArray(10, 50, false);
        PinnedQueryBuilder pqb = new PinnedQueryBuilder(new MatchAllQueryBuilder(), ids);
        List<String> addedIds = pqb.ids();
        int pos = 0;
        for (String key : addedIds) {
            assertEquals(ids[pos++], key);
        }
    }

    public void testDocInsertionOrderRetained() {
        Item[] items = randomArray(10, Item[]::new, () -> new Item(randomAlphaOfLength(64), randomAlphaOfLength(256)));
        PinnedQueryBuilder pqb = new PinnedQueryBuilder(new MatchAllQueryBuilder(), items);
        List<Item> addedDocs = pqb.docs();
        int pos = 0;
        for (Item item : addedDocs) {
            assertEquals(items[pos++], item);
        }
    }
}
