/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ViewMetadataIT extends AbstractEsqlIntegTestCase {

    private final List<String> createdViews = new ArrayList<>();

    private void createView(String name, String query) {
        assertAcked(client().execute(PutViewAction.INSTANCE, putViewRequest(name, query)));
        createdViews.add(name);
    }

    @After
    public void deleteViews() {
        for (var name : createdViews) {
            try {
                client().execute(DeleteViewAction.INSTANCE, deleteViewRequest(name)).actionGet();
            } catch (ResourceNotFoundException ignored) {} catch (Exception e) {
                logger.warn("view cleanup [{}] failed", name, e);
            }
        }
        createdViews.clear();
    }

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

    @Before
    public void createLogsIndices() {
        for (String name : List.of("view_it_logs_a", "view_it_logs_b", "view_it_logs_archive")) {
            if (indexExists(name)) {
                continue;
            }
            assertAcked(client().admin().indices().prepareCreate(name).setMapping("tag", "type=keyword").get());
            client().prepareIndex(name).setSource("tag", name).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        }
    }

    public void testNestedViewMetadataPassesThroughAllLevels() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_NO_BRANCHING", Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());
        createView("view_languages_nested_c_it", "FROM languages METADATA _index | EVAL viewC_index = _index");
        createView("view_languages_nested_b_it", "FROM view_languages_nested_c_it METADATA _index | EVAL viewB_index = _index");
        createView("view_languages_nested_a_it", "FROM view_languages_nested_b_it METADATA _index | EVAL viewA_index = _index");

        try (
            var response = run(
                "FROM view_languages_nested_a_it METADATA _index"
                    + " | SORT language_code"
                    + " | KEEP language_code, language_name, _index, viewC_index, viewB_index, viewA_index"
            )
        ) {
            var rows = getValuesList(response);
            assertThat(rows.size(), equalTo(4));
            for (var row : rows) {
                assertThat(row.get(2), equalTo("languages"));
                assertThat(row.get(3), equalTo("languages"));
                assertThat(row.get(4), equalTo("languages"));
                assertThat(row.get(5), equalTo("languages"));
            }
            assertThat(rows.get(0).get(0), equalTo(1));
            assertThat(rows.get(1).get(0), equalTo(2));
            assertThat(rows.get(2).get(0), equalTo(3));
            assertThat(rows.get(3).get(0), equalTo(4));
        }
    }

    /**
     * Same chain as {@link #testNestedViewMetadataPassesThroughAllLevels} but each view body is a
     * pure {@code FROM ... METADATA _index} with no pipeline — exercising the fast-path through
     * {@code ViewResolver.resolve()} that returns a bare {@code UnresolvedRelation} rather than a
     * {@code NamedSubquery}.
     */
    public void testNestedViewMetadataPassesThroughAllLevelsPureFROM() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_NO_BRANCHING", Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());
        createView("view_languages_pure_c_it", "FROM languages");
        createView("view_languages_pure_b_it", "FROM view_languages_pure_c_it METADATA _index");
        createView("view_languages_pure_a_it", "FROM view_languages_pure_b_it");

        try (
            var response = run("FROM view_languages_pure_a_it" + " | SORT language_code" + " | KEEP language_code, language_name, _index")
        ) {
            var rows = getValuesList(response);
            assertThat(rows.size(), equalTo(4));
            for (var row : rows) {
                assertThat(row.get(2), equalTo(null));
            }
            assertThat(rows.get(0).get(0), equalTo(1));
            assertThat(rows.get(1).get(0), equalTo(2));
            assertThat(rows.get(2).get(0), equalTo(3));
            assertThat(rows.get(3).get(0), equalTo(4));
        }
    }

    public void testNestedViewOuterMetadataNullWhenNotDeclaredInChain() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_NO_BRANCHING", Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());
        createView("view_languages_nested_c_it", "FROM languages METADATA _index | EVAL viewC_index = _index");
        createView("view_languages_nested_b_it", "FROM view_languages_nested_c_it METADATA _index | EVAL viewB_index = _index");
        createView("view_languages_nested_a_it", "FROM view_languages_nested_b_it METADATA _index | EVAL viewA_index = _index");

        try (
            var response = run(
                "FROM view_languages_nested_a_it METADATA _id"
                    + " | SORT language_code"
                    + " | LIMIT 1"
                    + " | KEEP language_code, language_name, _id"
            )
        ) {
            var rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat(rows.get(0).get(0), equalTo(1));
            assertThat(rows.get(0).get(2), nullValue());
        }
    }

    public void testViewAllMetadataInBodyPassesThrough() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_NO_BRANCHING", Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());
        createView("view_languages_all_metadata_it", "FROM languages METADATA _index, _id, _version, _score, _ignored, _index_mode");

        try (
            var response = run(
                "FROM view_languages_all_metadata_it METADATA _index, _id, _version, _score, _index_mode"
                    + " | SORT language_code"
                    + " | LIMIT 1"
                    + " | KEEP language_code, language_name, _index, _id, _version, _score, _index_mode"
            )
        ) {
            var rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            var row = rows.get(0);
            assertThat(row.get(0), equalTo(1));
            assertThat(row.get(1), equalTo("English"));
            assertThat(row.get(2), equalTo("languages"));
            assertThat(row.get(3), notNullValue());
            assertThat(row.get(4), equalTo(1L));
            assertThat(row.get(5), notNullValue());
            assertThat(row.get(6), equalTo("standard"));
        }
    }

    public void testViewMetadataWildcardPatternExpandsToNullColumns() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_NO_BRANCHING", Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());
        createView("view_languages_it", "FROM languages");

        try (
            var response = run(
                "FROM view_languages_it METADATA _in*"
                    + " | SORT language_code"
                    + " | LIMIT 1"
                    + " | KEEP language_code, _index, _index_mode"
            )
        ) {
            var rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            var row = rows.get(0);
            assertThat(row.get(0), equalTo(1));
            // _in* expanded to _index and _index_mode; the view body declares neither, so both are null
            assertThat(row.get(1), nullValue());
            assertThat(row.get(2), nullValue());
        }
    }

    public void testMetadataPatternInsideViewBodyExpandsAndCarriesValues() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_NO_BRANCHING", Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());
        createView("view_languages_pattern_it", "FROM languages METADATA _in*");

        try (
            var response = run(
                "FROM view_languages_pattern_it"
                    + " | SORT language_code"
                    + " | LIMIT 1"
                    + " | KEEP language_code, language_name, _index, _index_mode"
            )
        ) {
            var rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            var row = rows.get(0);
            assertThat(row.get(0), equalTo(1));
            assertThat(row.get(1), equalTo("English"));
            // _in* expanded to both _index and _index_mode, and both carry the real values
            assertThat(row.get(2), equalTo("languages"));
            assertThat(row.get(3), equalTo("standard"));
        }
    }

    public void testViewBodySubqueryMetadataSurvivesViewResolution() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_NO_BRANCHING", Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());
        createView("view_languages_subquery_meta_it", "FROM (FROM languages) METADATA _index");

        try (
            var response = run(
                "FROM view_languages_subquery_meta_it | SORT language_code | LIMIT 1 | KEEP language_code, language_name, _index"
            )
        ) {
            var rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            var row = rows.get(0);
            assertThat(row.get(0), equalTo(1));
            assertThat(row.get(1), equalTo("English"));
            assertThat(row.get(2), nullValue());
        }
    }

    public void testViewMetadataUnknownFieldRaisesVerificationException() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_NO_BRANCHING", Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());
        createView("view_languages_it", "FROM languages");

        VerificationException ex = expectThrows(VerificationException.class, () -> run("FROM view_languages_it METADATA _fake").close());
        // Same two problems a plain `FROM index METADATA _fake` reports: the unconsumed metadata request and the bad pattern.
        assertThat(
            ex.getMessage(),
            equalTo("Found 2 problems\nline 1:1: unresolved metadata fields: [?_fake]\nline 1:33: Unresolved metadata pattern [_fake]")
        );
    }

    public void testViewWithIndexPatternExclusionOmitsExcludedIndex() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_NO_BRANCHING", Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());
        createView("view_logs_exclusion_it", "FROM view_it_logs*, -view_it_logs_archive");

        try (var response = run("FROM view_logs_exclusion_it METADATA _index | KEEP tag, _index | SORT tag")) {
            var rows = getValuesList(response);
            assertThat(rows.size(), equalTo(2));
            assertThat(rows.get(0).get(0), equalTo("view_it_logs_a"));
            assertThat(rows.get(0).get(1), nullValue());
            assertThat(rows.get(1).get(0), equalTo("view_it_logs_b"));
            assertThat(rows.get(1).get(1), nullValue());
        }
    }

    public void testViewExclusionScopeDoesNotBleedToSiblingSource() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_BRANCHING", Cap.VIEWS_WITH_BRANCHING.isEnabled());
        createView("view_logs_exclusion_it", "FROM view_it_logs*, -view_it_logs_archive");

        try (var response = run("FROM view_logs_exclusion_it, view_it_logs_archive METADATA _index | KEEP tag, _index | SORT tag")) {
            var rows = getValuesList(response);
            // archive appears between a and b lexicographically
            assertThat(rows.size(), equalTo(3));
            assertThat(rows.get(0).get(0), equalTo("view_it_logs_a"));
            assertThat(rows.get(0).get(1), nullValue());
            assertThat(rows.get(1).get(0), equalTo("view_it_logs_archive"));
            assertThat(rows.get(1).get(1), equalTo("view_it_logs_archive"));
            assertThat(rows.get(2).get(0), equalTo("view_it_logs_b"));
            assertThat(rows.get(2).get(1), nullValue());
        }
    }

    public void testMixedViewAndIndexWithMetadataPatternExpandsPerBranch() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_BRANCHING", Cap.VIEWS_WITH_BRANCHING.isEnabled());
        createView("view_logs_exclusion_it", "FROM view_it_logs*, -view_it_logs_archive");

        try (var response = run("FROM view_logs_exclusion_it, view_it_logs_archive METADATA _inde* | KEEP tag, _index | SORT tag")) {
            var rows = getValuesList(response);
            assertThat(rows.size(), equalTo(3));
            assertThat(rows.get(0).get(0), equalTo("view_it_logs_a"));
            assertThat(rows.get(0).get(1), nullValue());
            assertThat(rows.get(1).get(0), equalTo("view_it_logs_archive"));
            assertThat(rows.get(1).get(1), equalTo("view_it_logs_archive"));
            assertThat(rows.get(2).get(0), equalTo("view_it_logs_b"));
            assertThat(rows.get(2).get(1), nullValue());
        }
    }

    public void testMixedViewAndIndexOrderNotImportant() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_BRANCHING", Cap.VIEWS_WITH_BRANCHING.isEnabled());
        createView("view_logs_b_it", "FROM view_it_logs_b");

        String projection = " | KEEP tag, _index | SORT tag";
        List<List<Object>> viewFirst;
        List<List<Object>> indexFirst;
        try (var response = run("FROM view_logs_b_it, view_it_logs_a METADATA _index" + projection)) {
            viewFirst = getValuesList(response);
        }
        try (var response = run("FROM view_it_logs_a, view_logs_b_it METADATA _index" + projection)) {
            indexFirst = getValuesList(response);
        }

        assertThat(indexFirst, equalTo(viewFirst));
        var metadataValues = viewFirst.stream().map(row -> row.get(1)).toList();
        assertThat(metadataValues, hasItem(nullValue()));
        assertThat(metadataValues, hasItem(notNullValue()));
    }

    public void testMultiSourceViewBodyMetadataPassesThrough() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_BRANCHING", Cap.VIEWS_WITH_BRANCHING.isEnabled());
        createView("view_multi_source_metadata_it", "FROM view_it_logs_a, view_it_logs_b METADATA _index");

        try (var response = run("FROM view_multi_source_metadata_it METADATA _index | KEEP tag, _index | SORT tag")) {
            var rows = getValuesList(response);
            assertThat(rows.size(), equalTo(2));
            assertThat(rows.get(0).get(0), equalTo("view_it_logs_a"));
            assertThat(rows.get(0).get(1), equalTo("view_it_logs_a"));
            assertThat(rows.get(1).get(0), equalTo("view_it_logs_b"));
            assertThat(rows.get(1).get(1), equalTo("view_it_logs_b"));
        }
    }

    private static PutViewAction.Request putViewRequest(String name, String query) {
        return new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query));
    }

    private static DeleteViewAction.Request deleteViewRequest(String name) {
        return new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { name });
    }
}
