/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.core.esql.EsqlFeatureFlags;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.hamcrest.Matcher;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;

public class ResolveIndexActionIT extends AbstractEsqlIntegTestCase {

    public void testResolveIndex() {
        assumeTrue("views only enabled when feature flag is set", EsqlFeatureFlags.ESQL_VIEWS_FEATURE_FLAG.isEnabled());
        assertAcked(client().admin().indices().create(new CreateIndexRequest("my-index-1")));
        assertAcked(client().admin().indices().create(new CreateIndexRequest("other-index-2")));

        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View("my-view-1", "ROW r=1"))
            )
        );

        verifyResolvedIndices(
            new ResolveIndexAction.Request(new String[] { "my-index-1" }),
            containsInAnyOrder("my-index-1"),
            emptyIterable(),
            emptyIterable()
        );
        verifyResolvedIndices(
            new ResolveIndexAction.Request(new String[] { "my-index-*" }),
            containsInAnyOrder("my-index-1"),
            emptyIterable(),
            emptyIterable()
        );

        // matched view is not returned, but it does not fail request
        expectThrows(
            IndexNotFoundException.class,
            containsString("no such index [my-view-1]"),
            () -> resolveIndices(new ResolveIndexAction.Request(new String[] { "my-view-1" }))
        );
        verifyResolvedIndices(
            new ResolveIndexAction.Request(new String[] { "my-*" }),
            containsInAnyOrder("my-index-1"),
            emptyIterable(),
            emptyIterable()
        );
    }

    private ResolveIndexAction.Response resolveIndices(ResolveIndexAction.Request request) {
        return client().admin().indices().resolveIndex(request).actionGet();
    }

    private void verifyResolvedIndices(
        ResolveIndexAction.Request request,
        Matcher<Iterable<? extends String>> indicesMatcher,
        Matcher<Iterable<? extends String>> aliasesMatcher,
        Matcher<Iterable<? extends String>> dataStreamsMatcher
    ) {
        var response = resolveIndices(request);
        assertThat(toStringList(response.getIndices()), indicesMatcher);
        assertThat(toStringList(response.getAliases()), aliasesMatcher);
        assertThat(toStringList(response.getDataStreams()), dataStreamsMatcher);
    }

    private static List<String> toStringList(List<? extends ResolveIndexAction.ResolvedIndexAbstraction> list) {
        return list.stream().map(ResolveIndexAction.ResolvedIndexAbstraction::getName).toList();
    }
}
