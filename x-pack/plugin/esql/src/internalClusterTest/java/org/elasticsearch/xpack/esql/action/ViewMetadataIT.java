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
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ViewMetadataIT extends AbstractEsqlIntegTestCase {

    private static final List<Map.Entry<String, String>> VIEW_DEFINITIONS = List.of(
        Map.entry("view_languages_nested_c_it", "FROM languages METADATA _index | EVAL viewC_index = _index"),
        Map.entry("view_languages_nested_b_it", "FROM view_languages_nested_c_it METADATA _index | EVAL viewB_index = _index"),
        Map.entry("view_languages_nested_a_it", "FROM view_languages_nested_b_it METADATA _index | EVAL viewA_index = _index"),
        Map.entry("view_languages_all_metadata_it", "FROM languages METADATA _index, _id, _version, _score, _ignored, _index_mode"),
        Map.entry("view_languages_it", "FROM languages")
    );

    private final List<String> createdViews = new ArrayList<>();

    @Before
    public void createViews() {
        if (Cap.OUTER_METADATA_NULL_INJECTION.isEnabled() == false || Cap.VIEWS_WITH_NO_BRANCHING.isEnabled() == false) {
            return;
        }
        for (var def : VIEW_DEFINITIONS) {
            assertAcked(client().execute(PutViewAction.INSTANCE, putViewRequest(def.getKey(), def.getValue())));
            createdViews.add(def.getKey());
        }
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

    public void testNestedViewMetadataPassesThroughAllLevels() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_NO_BRANCHING", Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());

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

    public void testNestedViewOuterMetadataNullWhenNotDeclaredInChain() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_NO_BRANCHING", Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());

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

    public void testViewMetadataWildcardPatternRaisesVerificationException() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_NO_BRANCHING", Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());

        VerificationException ex = expectThrows(VerificationException.class, () -> run("FROM view_languages_it METADATA _in*").close());
        assertThat(
            ex.getMessage(),
            equalTo("Found 2 problems\nline 1:1: unresolved metadata fields: [?_in*]\nline 1:33: Unresolved metadata pattern [_in*]")
        );
    }

    public void testViewMetadataUnknownFieldRaisesVerificationException() {
        assumeTrue("requires OUTER_METADATA_NULL_INJECTION", Cap.OUTER_METADATA_NULL_INJECTION.isEnabled());
        assumeTrue("requires VIEWS_WITH_NO_BRANCHING", Cap.VIEWS_WITH_NO_BRANCHING.isEnabled());

        VerificationException ex = expectThrows(VerificationException.class, () -> run("FROM view_languages_it METADATA _fake").close());
        assertThat(
            ex.getMessage(),
            equalTo("Found 2 problems\nline 1:1: unresolved metadata fields: [?_fake]\nline 1:33: Unresolved metadata pattern [_fake]")
        );
    }

    private static PutViewAction.Request putViewRequest(String name, String query) {
        return new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query));
    }

    private static DeleteViewAction.Request deleteViewRequest(String name) {
        return new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { name });
    }
}
