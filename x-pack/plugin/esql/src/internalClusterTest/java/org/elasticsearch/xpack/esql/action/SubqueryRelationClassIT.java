/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class SubqueryRelationClassIT extends AbstractEsqlIntegTestCase {

    @Before
    public void createLanguagesIndex() {
        if (indexExists("languages")) {
            return;
        }
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("languages")
                .setMapping("language_code", "type=integer", "language_name", "type=keyword")
                .get()
        );
        BulkRequestBuilder bulk = client().prepareBulk();
        bulk.add(prepareIndex("languages").setSource("language_code", 1, "language_name", "English"));
        bulk.add(prepareIndex("languages").setSource("language_code", 2, "language_name", "French"));
        bulk.add(prepareIndex("languages").setSource("language_code", 3, "language_name", "Spanish"));
        bulk.add(prepareIndex("languages").setSource("language_code", 4, "language_name", "German"));
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureGreen("languages");
    }

    public void testClassIsSubqueryForSingleSubquery() {
        assumeTrue("requires METADATA_CLASS_AND_NAME", Cap.METADATA_CLASS_AND_NAME.isEnabled());
        assumeTrue("requires SUBQUERY_IN_FROM_COMMAND", Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        try (var response = run("FROM (FROM languages) METADATA _class | KEEP language_code, _class | SORT language_code")) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(4));
            assertThat(rows.stream().map(r -> r.get(1)).toList(), everyItem(equalTo("subquery")));
        }
    }

    public void testNameIsNullForSingleSubquery() {
        assumeTrue("requires METADATA_CLASS_AND_NAME", Cap.METADATA_CLASS_AND_NAME.isEnabled());
        assumeTrue("requires SUBQUERY_IN_FROM_COMMAND", Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        try (var response = run("FROM (FROM languages) METADATA _name | KEEP language_code, _name | SORT language_code")) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(4));
            assertThat(rows.stream().map(r -> r.get(1)).toList(), everyItem(nullValue()));
        }
    }

    public void testBothClassAndNameForSingleSubquery() {
        assumeTrue("requires METADATA_CLASS_AND_NAME", Cap.METADATA_CLASS_AND_NAME.isEnabled());
        assumeTrue("requires SUBQUERY_IN_FROM_COMMAND", Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        try (var response = run("FROM (FROM languages) METADATA _class, _name | KEEP language_code, _class, _name | SORT language_code")) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(4));
            assertThat(rows.stream().map(r -> r.get(1)).toList(), everyItem(equalTo("subquery")));
            assertThat(rows.stream().map(r -> r.get(2)).toList(), everyItem(nullValue()));
        }
    }

    public void testTwoSubqueriesEachAnswerSubquery() {
        assumeTrue("requires METADATA_CLASS_AND_NAME", Cap.METADATA_CLASS_AND_NAME.isEnabled());
        assumeTrue("requires SUBQUERY_IN_FROM_COMMAND", Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        try (
            var response = run(
                "FROM (FROM languages | WHERE language_code <= 2), (FROM languages | WHERE language_code >= 3)"
                    + " METADATA _class | KEEP language_code, _class | SORT language_code"
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(4));
            assertThat(rows.stream().map(r -> r.get(1)).toList(), everyItem(equalTo("subquery")));
        }
    }

    public void testClassCountBySeparatesSubqueryFromIndex() {
        assumeTrue("requires METADATA_CLASS_AND_NAME", Cap.METADATA_CLASS_AND_NAME.isEnabled());
        assumeTrue("requires SUBQUERY_IN_FROM_COMMAND", Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        try (
            var response = run(
                "FROM languages, (FROM languages | KEEP language_code)"
                    + " METADATA _class | STATS n = COUNT(*) BY _class | KEEP _class, n | SORT _class"
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, equalTo(List.of(List.of("index", 4L), List.of("subquery", 4L))));
        }
    }

    public void testNameNullForSubqueryNonNullForIndex() {
        assumeTrue("requires METADATA_CLASS_AND_NAME", Cap.METADATA_CLASS_AND_NAME.isEnabled());
        assumeTrue("requires SUBQUERY_IN_FROM_COMMAND", Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        try (
            var response = run(
                "FROM languages, (FROM languages | KEEP language_code)"
                    + " METADATA _class, _name | WHERE _class == \"subquery\" | KEEP language_code, _name"
            )
        ) {
            assertThat(getValuesList(response).stream().map(r -> r.get(1)).toList(), everyItem(nullValue()));
        }
        try (
            var response = run(
                "FROM languages, (FROM languages | KEEP language_code)"
                    + " METADATA _class, _name | WHERE _class == \"index\" | KEEP language_code, _name"
            )
        ) {
            assertThat(getValuesList(response).stream().map(r -> r.get(1)).toList(), everyItem(equalTo("languages")));
        }
    }

    public void testMetadataClassInSubqueryBodyDoesNotChangeOuterClass() {
        assumeTrue("requires METADATA_CLASS_AND_NAME", Cap.METADATA_CLASS_AND_NAME.isEnabled());
        assumeTrue("requires SUBQUERY_IN_FROM_COMMAND", Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        try (
            var response = run(
                "FROM (FROM languages METADATA _class | RENAME _class AS inner_class)"
                    + " METADATA _class | KEEP language_code, inner_class, _class | SORT language_code"
            )
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(4));
            for (List<Object> row : rows) {
                assertThat(row.get(1), equalTo("index"));
                assertThat(row.get(2), equalTo("subquery"));
            }
        }
    }
}
