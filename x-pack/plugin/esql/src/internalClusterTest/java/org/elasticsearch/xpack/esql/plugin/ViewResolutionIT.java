/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;

public class ViewResolutionIT extends AbstractEsqlIntegTestCase {

    public void testResolveConcreteView() {
        assumeTrue("Requires views", EsqlCapabilities.Cap.VIEWS_CRUD_AS_INDEX_ACTIONS.isEnabled());

        indexRandom(true, false, prepareIndex("view-index").setSource(Map.of("id", randomIdentifier(), "source", "view-index")));
        try (var view = createView("test-view", "FROM view-index")) {
            try (var response = run(syncEsqlQueryRequest("FROM test-view"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "view-index");
            }
        }
    }

    public void testResolvePattern() {
        assumeTrue("Requires views", EsqlCapabilities.Cap.VIEWS_CRUD_AS_INDEX_ACTIONS.isEnabled());

        indexRandom(
            true,
            false,
            prepareIndex("view-index").setSource(Map.of("id", randomIdentifier(), "source", "view-index")),
            prepareIndex("test-index").setSource(Map.of("id", randomIdentifier(), "source", "test-index"))
        );
        try (var view = createView("test-view", "FROM view-index")) {
            try (var response = run(syncEsqlQueryRequest("FROM test-*"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "view-index", "test-index");
            }
        }
    }

    public void testDotPrefixedViews() {
        assertAcked(client().admin().indices().prepareCreate("regular-index-1"));
        indexRandom(true, false, prepareIndex("regular-index-1").setSource(Map.of("id", randomIdentifier(), "source", "regular-index-1")));

        try (var view = createView(".non-hidden-view", "FROM regular-index-1")) {
            try (var response = run(syncEsqlQueryRequest("FROM .non-hidden-view"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "regular-index-1");
            }
            try (var response = run(syncEsqlQueryRequest("FROM *-view"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "regular-index-1");
            }
            try (var response = run(syncEsqlQueryRequest("FROM .non-hidden-*"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "regular-index-1");
            }
            try (var response = run(syncEsqlQueryRequest("FROM *"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "regular-index-1", "regular-index-1");// matched index and view
            }
        }
    }

    public void testViewWithIndexComponentSelectors() {
        assumeTrue("Requires index component selectors", EsqlCapabilities.Cap.INDEX_COMPONENT_SELECTORS.isEnabled());
        assumeTrue("Requires views", EsqlCapabilities.Cap.VIEWS_CRUD_AS_INDEX_ACTIONS.isEnabled());

        indexRandom(true, false, prepareIndex("view-index").setSource(Map.of("id", randomIdentifier(), "source", "view-index")));
        try (var view = createView("test-view", "FROM view-index")) {
            // view::data is equivalent to the plain view name
            try (var response = run(syncEsqlQueryRequest("FROM test-view::data"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "view-index");
            }
            // view::failures is not supported; the view has no failure component
            expectThrows(
                VerificationException.class,
                containsString("Unknown index [test-view::failures]"),
                () -> run(syncEsqlQueryRequest("FROM test-view::failures")).close()
            );
        }
    }

    private Releasable createView(String name, String query) {
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query))
            )
        );
        return () -> assertAcked(
            client().execute(
                DeleteViewAction.INSTANCE,
                new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { name })
            )
        );
    }

    private static void assertResultConcreteIndices(EsqlQueryResponse response, Object... indices) {
        assertColumnContainsInAnyOrder(response, "source", indices);
    }
}
