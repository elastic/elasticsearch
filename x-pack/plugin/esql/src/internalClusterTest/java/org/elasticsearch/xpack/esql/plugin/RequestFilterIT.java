/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

public class RequestFilterIT extends AbstractEsqlIntegTestCase {

    public void testRequestFilterWithView() {
        client().prepareBulk()
            .add(client().prepareIndex("index-1").setSource("f1", 1, "source", "index-1"))
            .add(client().prepareIndex("index-2").setSource("f2", 2, "source", "index-2"))
            .add(client().prepareIndex("index-3").setSource("f3", 3, "source", "index-3"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var view1 = createView("view-2", randomize("FROM index-2")); var view2 = createView("view-3", randomize("FROM index-3"))) {
            // match all
            try (var response = run(syncEsqlQueryRequest("FROM index-1,view-2,view-3").filter(new MatchAllQueryBuilder()))) {
                assertColumnContainsInAnyOrder(response, "source", "index-1", "index-2", "index-3");
            }
            // match index from toplevel query
            try (var response = run(syncEsqlQueryRequest("FROM index-1,view-2,view-3").filter(new TermQueryBuilder("f1", 1)))) {
                assertColumnContainsInAnyOrder(response, "source", "index-1");
            }
            // match index from view
            try (var response = run(syncEsqlQueryRequest("FROM index-1,view-2,view-3").filter(new TermQueryBuilder("f2", 2)))) {
                assertColumnContainsInAnyOrder(response, "source", "index-2");
            }
        }
        try (
            var view1 = createView("view-2", randomize("FROM index-2") + " | EVAL f2=3");
            var view2 = createView("view-3", randomize("FROM index-3") + " | EVAL f2=2");
            var response = run(
                syncEsqlQueryRequest("FROM index-1,view-2,view-3")//
                    .filter(new TermQueryBuilder("f2", 2))
            )
        ) {
            // filter is executed against final view output as if it is index. This must take into account fields added/changed by evals
            // Uncomment once https://github.com/elastic/elasticsearch/pull/156879 is merged
            // assertColumnContainsInAnyOrder(response, "source", "index-3");
        }
    }

    public void testRequestFilterWithSubquery() {
        client().prepareBulk()
            .add(client().prepareIndex("index-1").setSource("f1", 1, "source", "index-1"))
            .add(client().prepareIndex("index-2").setSource("f2", 2, "source", "index-2"))
            .add(client().prepareIndex("index-3").setSource("f3", 3, "source", "index-3"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // match all
        try (
            var response = run(
                syncEsqlQueryRequest("FROM index-1,(" + randomize("FROM index-2") + "),(" + randomize("FROM index-3") + ")") //
                    .filter(new MatchAllQueryBuilder())
            )
        ) {
            assertColumnContainsInAnyOrder(response, "source", "index-1", "index-2", "index-3");
        }
        // match index from toplevel query
        try (
            var response = run(
                syncEsqlQueryRequest("FROM index-1,(" + randomize("FROM index-2") + "),(" + randomize("FROM index-3") + ")") //
                    .filter(new TermQueryBuilder("f1", 1))
            )
        ) {
            assertColumnContainsInAnyOrder(response, "source", "index-1");
        }
        // match index from subquery
        try (
            var response = run(
                syncEsqlQueryRequest("FROM index-1,(" + randomize("FROM index-2") + "),(" + randomize("FROM index-3") + ")") //
                    .filter(new TermQueryBuilder("f2", 2))
            )
        ) {
            assertColumnContainsInAnyOrder(response, "source", "index-2");
        }
        // match index from subquery with shadowed fields
        try (
            var response = run(
                syncEsqlQueryRequest(
                    "FROM index-1,(" + randomize("FROM index-2") + " | EVAL f2=3),(" + randomize("FROM index-3") + " | EVAL f2=2)"
                ).filter(new TermQueryBuilder("f2", 2))
            )
        ) {
            // filter is executed against original index fields, not ones created by evals
            assertColumnContainsInAnyOrder(response, "source", "index-2");
        }
    }

    private static String randomize(String query) {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> query;
            case 1 -> query + " | WHERE true";
            case 2 -> query + " | LIMIT 1";
            case 3 -> query + " | LIMIT 1 BY source";
            default -> throw new AssertionError("unreachable");
        };
    }

    private static Releasable createView(String name, String query) {
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
}
