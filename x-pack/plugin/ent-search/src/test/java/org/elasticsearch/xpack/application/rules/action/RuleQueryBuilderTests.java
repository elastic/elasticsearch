/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.apache.lucene.search.Query;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xpack.application.EnterpriseSearch;
import org.elasticsearch.xpack.application.rules.RuleQueryBuilder;

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
        return new RuleQueryBuilder(
            new MatchAllQueryBuilder(),
            randomMap(1, 3, () -> new Tuple<>(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))),
            randomList(1, 2, String::new)
        );
    }

    @Override
    protected void doAssertLuceneQuery(RuleQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        // TODO - Figure out what needs to be validated here.
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> classpathPlugins = new ArrayList<>();
        classpathPlugins.add(EnterpriseSearch.class);
        return classpathPlugins;
    }

    public void testIllegalArguments() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new RuleQueryBuilder(new MatchAllQueryBuilder(), null, Collections.singletonList("rulesetId"))
        );
        expectThrows(IllegalArgumentException.class, () -> new RuleQueryBuilder(new MatchAllQueryBuilder(), Map.of("foo", "bar"), null));
        expectThrows(
            IllegalArgumentException.class,
            () -> new RuleQueryBuilder(new MatchAllQueryBuilder(), Map.of("foo", "bar"), Collections.emptyList())
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RuleQueryBuilder(null, Map.of("foo", "bar"), Collections.singletonList("rulesetId"))
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RuleQueryBuilder(null, Collections.emptyMap(), Collections.singletonList("rulesetId"))
        );
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
                  "query_string": "elastic"
                },
                "ruleset_ids": [ "ruleset1", "ruleset2" ]
              }
            }""";

        RuleQueryBuilder queryBuilder = (RuleQueryBuilder) parseQuery(query);
        checkGeneratedJson(query, queryBuilder);

        assertEquals(query, 2, queryBuilder.rulesetIds().size());
        assertEquals(query, "elastic", queryBuilder.matchCriteria().get("query_string"));
        assertThat(queryBuilder.organicQuery(), instanceOf(TermQueryBuilder.class));
    }

    // Copied from PinnedQueryBuilderTests - TODO go through these and see what test cases we may want to copy
    // /**
    // * test that unknown query names in the clauses throw an error
    // */
    // public void testUnknownQueryName() {
    // String query = "{\"rule_query\" : {\"organic\" : { \"unknown_query\" : { } } } }";
    //
    // ParsingException ex = expectThrows(ParsingException.class, () -> parseQuery(query));
    // assertEquals("[1:46] [rule_query] failed to parse field [organic]", ex.getMessage());
    // }
    //
    // public void testIdsRewrite() throws IOException {
    // RuleQueryBuilder ruleQueryBuilder = new RuleQueryBuilder(new TermQueryBuilder("foo", 1),
    // Collections.singletonList("bar"), Collections.emptyMap());
    // QueryBuilder rewritten = ruleQueryBuilder.rewrite(createSearchExecutionContext());
    // assertThat(rewritten, instanceOf(RuleQueryBuilder.class));
    // }
    //
    // public void testDocsRewrite() throws IOException {
    // RuleQueryBuilder RuleQueryBuilder = new RuleQueryBuilder(new TermQueryBuilder("foo", 1), new Item("test", "1"));
    // QueryBuilder rewritten = RuleQueryBuilder.rewrite(createSearchExecutionContext());
    // assertThat(rewritten, instanceOf(RuleQueryBuilder.class));
    // }
    //
    // @Override
    // public void testMustRewrite() throws IOException {
    // SearchExecutionContext context = createSearchExecutionContext();
    // context.setAllowUnmappedFields(true);
    // RuleQueryBuilder queryBuilder = new RuleQueryBuilder(new TermQueryBuilder("unmapped_field", "42"), "42");
    // IllegalStateException e = expectThrows(IllegalStateException.class, () -> queryBuilder.toQuery(context));
    // assertEquals("Rewrite first", e.getMessage());
    // }

    // public void testIdInsertionOrderRetained() {
    // String[] ids = generateRandomStringArray(10, 50, false);
    // RuleQueryBuilder pqb = new RuleQueryBuilder(new MatchAllQueryBuilder(), ids);
    // List<String> addedIds = pqb.ids();
    // int pos = 0;
    // for (String key : addedIds) {
    // assertEquals(ids[pos++], key);
    // }
    // }
    //
    // public void testDocInsertionOrderRetained() {
    // Item[] items = randomArray(10, Item[]::new, () -> new Item(randomAlphaOfLength(64), randomAlphaOfLength(256)));
    // RuleQueryBuilder pqb = new RuleQueryBuilder(new MatchAllQueryBuilder(), items);
    // List<Item> addedDocs = pqb.docs();
    // int pos = 0;
    // for (Item item : addedDocs) {
    // assertEquals(items[pos++], item);
    // }
    // }
}
