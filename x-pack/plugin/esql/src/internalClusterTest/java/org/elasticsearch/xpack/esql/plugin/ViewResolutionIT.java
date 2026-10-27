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
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class ViewResolutionIT extends AbstractEsqlIntegTestCase {

    public void testResolveConcreteView() {
        indexRandom(true, false, prepareIndex("view-index").setSource(Map.of("id", randomIdentifier(), "source", "view-index")));
        try (var view = createView("test-view", "FROM view-index")) {
            try (var response = run(syncEsqlQueryRequest("FROM test-view"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "view-index");
            }
            try (var response = run(syncEsqlQueryRequest("FROM test-view,-view-index"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "view-index"); // exclusion is not carried into inner view
            }
        }
    }

    public void testResolvePattern() {
        indexRandom(
            true,
            false,
            prepareIndex("view-index").setSource(Map.of("id", randomIdentifier(), "source", "view-index")),
            prepareIndex("test-index").setSource(Map.of("id", randomIdentifier(), "source", "test-index"))
        );
        try (var view = createView("test-view", "FROM view-index")) {
            try (var response = run(syncEsqlQueryRequest("FROM test-*"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "test-index"); // no views by default
            }
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=false; FROM test-*"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "test-index"); // views are opt-out
            }
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM test-*"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "test-index", "view-index"); // views are opt in
            }
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM test-*,-test-view"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "test-index"); // views can be excluded using concrete expression
            }
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM test-*,-*-view"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "test-index"); // views can be excluded using wildcard expression
            }
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM test-*,-*-index"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "view-index"); // index can be excluded using wildcard expression
            }
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM test-*,-view-index"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "test-index", "view-index");// does not exclude inner index by concrete expression
            }
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM test-*,-view-index"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "test-index", "view-index");// does not exclude inner index by wildcard expression
            }
        }
    }

    public void testWildcardsMatchViewsWithMixedViewIndexResolution() {
        indexRandom(
            true,
            false,
            prepareIndex("view-index").setSource(Map.of("id", randomIdentifier(), "source", "view-index")),
            prepareIndex("test-index").setSource(Map.of("id", randomIdentifier(), "source", "test-index"))
        );
        try (var view = createView("test-view", "FROM view-index")) {
            // concrete index and view
            try (var response = run(syncEsqlQueryRequest("FROM test-view,test-index"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "test-index", "view-index");
            }
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM test-view,test-index"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "test-index", "view-index");
            }
            // concrete index and view and common pattern
            try (var response = run(syncEsqlQueryRequest("FROM test-view,test-index,test-*"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "test-index", "view-index");
            }
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM test-view,test-index,test-*"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "test-index", "view-index");
            }
            // concrete view and index pattern
            try (var response = run(syncEsqlQueryRequest("FROM test-view,test-*"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "test-index", "view-index");
            }
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM test-view,test-*"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "test-index", "view-index");
            }
            // concrete index and view pattern
            try (var response = run(syncEsqlQueryRequest("FROM test-index,test-*"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "test-index");
            }
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM test-index,test-*"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "test-index", "view-index");
            }
        }
    }

    public void testWildcardsMatchViewsDoesNotApplyInsideNestedViews() {
        try (
            var outer = createView("outer", "FROM middle");
            var middle = createView("middle", "FROM inner*");
            var inner = createView("inner", "ROW source=\"inner\"");
        ) {
            // The caller's wildcards_match_views stops at the view boundary (settings_scoped_at_view_boundaries):
            // the wildcard inside middle's body only matches indices — none here — and never the inner view, so
            // the setting makes no difference. Views still compose by exact name (outer's body reaches middle).
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM outer"))) {
                assertOk(response);
                assertEmpty(response);
            }
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=false; FROM outer"))) {
                assertOk(response);
                assertEmpty(response);
            }
        }
    }

    public void testReservedViews() {
        indexRandom(
            true,
            false,
            prepareIndex("reserved-index").setSource(Map.of("id", randomIdentifier(), "source", "reserved-index")),
            prepareIndex("regular-index").setSource(Map.of("id", randomIdentifier(), "source", "regular-index"))
        );
        try (
            var regularView = createView("regular-view", "FROM regular-index");
            var systemView = createView(".reserved-view", "FROM reserved-index", null, true)
        ) {
            try (var response = run(syncEsqlQueryRequest("FROM .reserved-view"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "reserved-index"); // concrete name resolves system view
            }
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM *-view"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "reserved-index", "regular-index"); // wildcard resolves reserved
            }
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM .reserved-*"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "reserved-index");
            }
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM *"))) {
                assertOk(response);
                // system-index & regular-index are matched both directly and via views
                assertResultConcreteIndices(response, "reserved-index", "reserved-index", "regular-index", "regular-index");
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
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM *-view"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "regular-index-1");
            }
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM .non-hidden-*"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "regular-index-1");
            }
            try (var response = run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM *"))) {
                assertOk(response);
                assertResultConcreteIndices(response, "regular-index-1", "regular-index-1");// matched index and view
            }
        }
    }

    public void testViewWithIndexComponentSelectors() {
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

    public void testUnknownIndexErrorDescribesViews() {
        assertAcked(client().admin().indices().prepareCreate("index1").setMapping("f1", "type=integer"));
        indexRandom(true, false, prepareIndex("index1").setSource(Map.of("f1", 42)));
        try (
            var unresolvable = createView("unresolvable", randomFrom("FROM missing", "FROM missing | KEEP f1"));
            var outer = createView("outer", "FROM unresolvable | KEEP f1");
            var resolvable = createView("resolvable", "FROM index1 | KEEP f1")
        ) {
            expectThrows(
                VerificationException.class,
                containsString("Unknown index [missing] referenced by view [unresolvable]"),
                () -> run(syncEsqlQueryRequest("FROM unresolvable")).close()
            );
            expectThrows(
                VerificationException.class,
                containsString("Unknown index [missing] referenced by view [outer -> unresolvable]"),
                () -> run(syncEsqlQueryRequest("FROM index1, outer")).close()
            );
            expectThrows(
                VerificationException.class,
                containsString("Unknown index [missing] referenced by view [unresolvable]"),
                () -> run(syncEsqlQueryRequest("SET wildcards_match_views=true; FROM unresolv*")).close()
            );
            expectThrows(
                VerificationException.class,
                containsString("Unknown index [missing] referenced by view [unresolvable]"),
                () -> run(syncEsqlQueryRequest("FROM index1 | WHERE f1 IN (FROM unresolvable | KEEP f1)")).close()
            );
            expectThrows(
                VerificationException.class,
                containsString("Unknown index [missing] referenced by view [unresolvable]"),
                () -> run(syncEsqlQueryRequest("FROM missing, unresolvable")).close()
            );
            // "FROM missing" is compacted into the outer pattern and keeps query order. "FROM missing | KEEP f1" stays a separate
            // pattern, and separate patterns are registered in pattern-text order.
            expectThrows(
                VerificationException.class,
                anyOf(
                    containsString("Unknown index [other-missing,missing] ([missing] referenced by view [unresolvable])"),
                    containsString("Unknown index [missing,other-missing] ([missing] referenced by view [unresolvable])")
                ),
                () -> run(syncEsqlQueryRequest("FROM other-missing, unresolvable")).close()
            );
            expectThrows(
                VerificationException.class,
                allOf(containsString("Unknown index [missing]"), not(containsString("referenced by view"))),
                () -> run(syncEsqlQueryRequest("FROM missing, resolvable")).close()
            );
        }
    }

    private Releasable createView(String name, String query) {
        return createView(name, query, null, false);
    }

    private Releasable createView(String name, String query, String description, boolean reserved) {
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query, description, reserved))
            )
        );
        return () -> assertAcked(
            client().execute(
                DeleteViewAction.INSTANCE,
                new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { name }, reserved)
            )
        );
    }

    private static void assertResultConcreteIndices(EsqlQueryResponse response, Object... indices) {
        assertColumnContainsInAnyOrder(response, "source", indices);
    }
}
