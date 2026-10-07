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

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.nCopies;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

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

    public void testClassAndNameForSingleSubquery() {
        assumeTrue("requires METADATA_CLASS_AND_NAME", Cap.METADATA_CLASS_AND_NAME.isEnabled());
        assumeTrue("requires SUBQUERY_IN_FROM_COMMAND", Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        try (var response = run("FROM (FROM languages) METADATA _class, _name | SORT language_code")) {
            assertThat(column(response, "_class"), equalTo(nCopies(4, "subquery")));
            assertThat(column(response, "_name"), equalTo(nCopies(4, null)));
        }
    }

    public void testClassAndNameForTwoSubqueries() {
        assumeTrue("requires METADATA_CLASS_AND_NAME", Cap.METADATA_CLASS_AND_NAME.isEnabled());
        assumeTrue("requires SUBQUERY_IN_FROM_COMMAND", Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        try (
            var response = run(
                "FROM (FROM languages | WHERE language_code <= 2), (FROM languages | WHERE language_code >= 3)"
                    + " METADATA _class, _name | SORT language_code"
            )
        ) {
            assertThat(column(response, "_class"), equalTo(nCopies(4, "subquery")));
            assertThat(column(response, "_name"), equalTo(nCopies(4, null)));
        }
    }

    public void testClassAndNameSeparateSubqueryFromIndex() {
        assumeTrue("requires METADATA_CLASS_AND_NAME", Cap.METADATA_CLASS_AND_NAME.isEnabled());
        assumeTrue("requires SUBQUERY_IN_FROM_COMMAND", Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        try (
            var response = run(
                "FROM languages, (FROM languages | KEEP language_code)"
                    + " METADATA _class, _name | STATS n = COUNT(*) BY _class, _name | SORT _class"
            )
        ) {
            assertThat(column(response, "_class"), equalTo(List.of("index", "subquery")));
            assertThat(column(response, "_name"), equalTo(Arrays.asList("languages", null)));
            assertThat(column(response, "n"), equalTo(List.of(4L, 4L)));
        }
    }

    public void testClassAndNameInSubqueryBodyDoNotChangeOuterValues() {
        assumeTrue("requires METADATA_CLASS_AND_NAME", Cap.METADATA_CLASS_AND_NAME.isEnabled());
        assumeTrue("requires SUBQUERY_IN_FROM_COMMAND", Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        try (
            var response = run(
                "FROM (FROM languages METADATA _class, _name | RENAME _class AS inner_class, _name AS inner_name)"
                    + " METADATA _class, _name | SORT language_code"
            )
        ) {
            assertThat(column(response, "inner_class"), equalTo(nCopies(4, "index")));
            assertThat(column(response, "inner_name"), equalTo(nCopies(4, "languages")));
            assertThat(column(response, "_class"), equalTo(nCopies(4, "subquery")));
            assertThat(column(response, "_name"), equalTo(nCopies(4, null)));
        }
    }
}
