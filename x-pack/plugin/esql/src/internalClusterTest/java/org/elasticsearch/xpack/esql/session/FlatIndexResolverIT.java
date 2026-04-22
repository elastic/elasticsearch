/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsResponse;
import org.elasticsearch.xpack.esql.index.IndexResolution;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class FlatIndexResolverIT extends AbstractEsqlIntegTestCase {

    public void testResolveRequiredAndOptionalFlatIndices() {

        prepareCreate("index-1").setMapping("f1", "type=keyword").get();
        prepareCreate("index-2").setMapping("f2", "type=keyword").get();

        // only required index requested
        {
            var result = resolveFlatIndices("index-1", null);
            assertThat(result.isValid(), equalTo(true));
            assertThat(result.resolvedIndices(), containsInAnyOrder("index-1"));
            assertThat(result.get().mapping().keySet(), containsInAnyOrder("f1"));
        }

        // only optional index requested
        {
            var lenientCaps = resolveLenient("index-2");
            var result = resolveFlatIndices("", lenientCaps);
            assertThat(result.isValid(), equalTo(true));
            assertThat(result.resolvedIndices(), containsInAnyOrder("index-2"));
            assertThat(result.get().mapping().keySet(), containsInAnyOrder("f2"));
        }

        // required and optional index found
        {
            var lenientCaps = resolveLenient("index-2");
            var result = resolveFlatIndices("index-1", lenientCaps);
            assertThat(result.isValid(), equalTo(true));
            assertThat(result.resolvedIndices(), containsInAnyOrder("index-1", "index-2"));
            assertThat(result.get().mapping().keySet(), containsInAnyOrder("f1", "f2"));
        }

        // only required index found, optional missing
        {
            var lenientCaps = resolveLenient("index-3");
            var result = resolveFlatIndices("index-1", lenientCaps);
            assertThat(result.isValid(), equalTo(true));
            assertThat(result.resolvedIndices(), containsInAnyOrder("index-1"));
            assertThat(result.get().mapping().keySet(), containsInAnyOrder("f1"));
        }

        // required index is not found
        expectThrows(IndexNotFoundException.class, containsString("no such index [index-3]"), () -> resolveFlatIndices("index-3", null));
    }

    private FieldCapabilitiesResponse resolveLenient(String optionalPattern) {
        var response = FlatIndexResolverIT.<EsqlResolveFieldsResponse>run(
            future -> new IndexResolver(client()).resolveLenientOnly(optionalPattern, null, Set.of("*"), null, future)
        ).actionGet();
        return IndexResolver.mergeLenientResponses(List.of(response));
    }

    private IndexResolution resolveFlatIndices(String required, FieldCapabilitiesResponse additionalLenientCaps) {
        return FlatIndexResolverIT.<Versioned<IndexResolution>>run(
            future -> new IndexResolver(client()).resolveMainFlatIndicesVersioned(
                required,
                additionalLenientCaps,
                null,
                Set.of("*"),
                null,
                false,
                TransportVersion.current(),
                false,
                false,
                false,
                false,
                future
            )
        ).actionGet().inner();
    }

    private static <T> PlainActionFuture<T> run(Consumer<PlainActionFuture<T>> action) {
        PlainActionFuture<T> future = new PlainActionFuture<>();
        action.accept(future);
        return future;
    }
}
