/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.json.JsonStringEncoder;
import org.elasticsearch.xpack.application.rules.RuleQueryBuilder;
import org.elasticsearch.xpack.searchbusinessrules.SearchBusinessRules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;

public class RuleQueryBuilderTests extends AbstractQueryTestCase<RuleQueryBuilder> {
    @Override
    protected RuleQueryBuilder doCreateTestQueryBuilder() {
        return new RuleQueryBuilder(createRandomQuery(),
            randomList(1, 10, String::new),
            randomMap(1, 3, () -> new Tuple<>(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))));
    }

    private static QueryBuilder createRandomQuery() {
        if (randomBoolean()) {
            return new MatchAllQueryBuilder();
        } else {
            return createTestTermQueryBuilder();
        }
    }

    private static QueryBuilder createTestTermQueryBuilder() {
        String fieldName = null;
        Object value;
        switch (randomIntBetween(0, 3)) {
            case 0 -> {
                if (randomBoolean()) {
                    fieldName = BOOLEAN_FIELD_NAME;
                }
                value = randomBoolean();
            }
            case 1 -> {
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
            }
            case 2 -> {
                if (randomBoolean()) {
                    fieldName = INT_FIELD_NAME;
                }
                value = randomInt(10000);
            }
            case 3 -> {
                if (randomBoolean()) {
                    fieldName = DOUBLE_FIELD_NAME;
                }
                value = randomDouble();
            }
            default -> throw new UnsupportedOperationException();
        }

        if (fieldName == null) {
            fieldName = randomAlphaOfLengthBetween(1, 10);
        }
        return new TermQueryBuilder(fieldName, value);
    }

    @Override
    protected void doAssertLuceneQuery(RuleQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
//        if (queryBuilder.ids().size() == 0 && queryBuilder.docs().size() == 0) {
//            assertThat(query, instanceOf(CappedScoreQuery.class));
//        } else {
            // Have IDs/docs and an organic query - uses DisMax
            assertThat(query, instanceOf(DisjunctionMaxQuery.class));
//        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> classpathPlugins = new ArrayList<>();
        classpathPlugins.add(SearchBusinessRules.class);
        return classpathPlugins;
    }

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () ->
            new RuleQueryBuilder(new MatchAllQueryBuilder(), Collections.singletonList("rulesetId"), null));
        expectThrows(IllegalArgumentException.class, () ->
            new RuleQueryBuilder(new MatchAllQueryBuilder(), null, Map.of("foo", "bar")));
        expectThrows(IllegalArgumentException.class, () ->
            new RuleQueryBuilder(new MatchAllQueryBuilder(), Collections.emptyList(), Map.of("foo", "bar")));
        expectThrows(IllegalArgumentException.class, () ->
            new RuleQueryBuilder(null, Collections.singletonList("rulesetId"), Map.of("foo", "bar")));
        expectThrows(IllegalArgumentException.class, () ->
            new RuleQueryBuilder(null, Collections.singletonList("rulesetId"), Collections.emptyMap()));
    }

    public void testFromJson() throws IOException {
        String query = """
            {
              "rule_query": {
                "organic": {
                  "term": {
                    "tag": {
                      "value": "search"
                    }
                  }
                },
                "match_criteria": {
                  "query_string": [ "elastic" ]
                }
                "ruleset_ids": [ "ruleset1", "ruleset2" ]
              }
            }""";

        RuleQueryBuilder queryBuilder = (RuleQueryBuilder) parseQuery(query);
        checkGeneratedJson(query, queryBuilder);

        assertEquals(query, 2, queryBuilder.rulesetIds().size());
        assertEquals(query, "elastic", queryBuilder.matchCriteria().get("query_string"));
        assertThat(queryBuilder.organicQuery(), instanceOf(TermQueryBuilder.class));
    }

//    /**
//     * test that unknown query names in the clauses throw an error
//     */
//    public void testUnknownQueryName() {
//        String query = "{\"rule_query\" : {\"organic\" : { \"unknown_query\" : { } } } }";
//
//        ParsingException ex = expectThrows(ParsingException.class, () -> parseQuery(query));
//        assertEquals("[1:46] [rule_query] failed to parse field [organic]", ex.getMessage());
//    }
//
//    public void testIdsRewrite() throws IOException {
//        RuleQueryBuilder ruleQueryBuilder = new RuleQueryBuilder(new TermQueryBuilder("foo", 1),
//            Collections.singletonList("bar"), Collections.emptyMap());
//        QueryBuilder rewritten = ruleQueryBuilder.rewrite(createSearchExecutionContext());
//        assertThat(rewritten, instanceOf(RuleQueryBuilder.class));
//    }
//
//    public void testDocsRewrite() throws IOException {
//        RuleQueryBuilder RuleQueryBuilder = new RuleQueryBuilder(new TermQueryBuilder("foo", 1), new Item("test", "1"));
//        QueryBuilder rewritten = RuleQueryBuilder.rewrite(createSearchExecutionContext());
//        assertThat(rewritten, instanceOf(RuleQueryBuilder.class));
//    }
//
//    @Override
//    public void testMustRewrite() throws IOException {
//        SearchExecutionContext context = createSearchExecutionContext();
//        context.setAllowUnmappedFields(true);
//        RuleQueryBuilder queryBuilder = new RuleQueryBuilder(new TermQueryBuilder("unmapped_field", "42"), "42");
//        IllegalStateException e = expectThrows(IllegalStateException.class, () -> queryBuilder.toQuery(context));
//        assertEquals("Rewrite first", e.getMessage());
//    }

//    public void testIdInsertionOrderRetained() {
//        String[] ids = generateRandomStringArray(10, 50, false);
//        RuleQueryBuilder pqb = new RuleQueryBuilder(new MatchAllQueryBuilder(), ids);
//        List<String> addedIds = pqb.ids();
//        int pos = 0;
//        for (String key : addedIds) {
//            assertEquals(ids[pos++], key);
//        }
//    }
//
//    public void testDocInsertionOrderRetained() {
//        Item[] items = randomArray(10, Item[]::new, () -> new Item(randomAlphaOfLength(64), randomAlphaOfLength(256)));
//        RuleQueryBuilder pqb = new RuleQueryBuilder(new MatchAllQueryBuilder(), items);
//        List<Item> addedDocs = pqb.docs();
//        int pos = 0;
//        for (Item item : addedDocs) {
//            assertEquals(items[pos++], item);
//        }
//    }
}
