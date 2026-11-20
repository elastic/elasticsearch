/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsAction;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsRequest;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.session.EsqlSession.PreAnalysisResult;
import org.elasticsearch.xpack.esql.view.IndexViewOrigins;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Whether a query reads nested fields travels as {@link PreAnalysisResult#nestedScope()}, next to the field names rather than
 * inside them. Only the resolution of the main source of that query asks field caps for nested fields.
 */
public class NestedScopeResolutionTests extends ESTestCase {

    private static final Set<String> NESTED_NAMES = Set.of("items", "items.*", "items.sku");

    private ThreadPool threadPool;
    private AtomicReference<FieldCapabilitiesRequest> captured;
    private IndexResolver resolver;

    @Before
    public void setUpResolver() {
        threadPool = new TestThreadPool(getTestName());
        captured = new AtomicReference<>();
        NoOpClient client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assertThat(action, is(EsqlResolveFieldsAction.TYPE));
                captured.set(((EsqlResolveFieldsRequest) request).fieldCapsRequest());
                listener.onFailure(new IllegalStateException("request captured"));
            }
        };
        resolver = new IndexResolver(client, () -> true);
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testMainIndicesKeepTheNestedFilterWithoutNestedScope() {
        resolveMain(NESTED_NAMES, false, false);
        assertThat(captured.get().filters(), arrayContaining("-nested"));
        assertThat(Set.of(captured.get().fields()), equalTo(NESTED_NAMES));
    }

    public void testMainIndicesDropTheNestedFilterWithNestedScope() {
        resolveMain(NESTED_NAMES, false, true);
        assertThat(captured.get().filters(), is(new String[0]));
        assertThat(Set.of(captured.get().fields()), equalTo(NESTED_NAMES));
    }

    public void testNestedScopeKeepsTheDimensionFilterIndependent() {
        resolveMain(NESTED_NAMES, true, true);
        assertThat(captured.get().filters(), arrayContaining("+dimension"));
        resolveMain(NESTED_NAMES, true, false);
        assertThat(captured.get().filters(), arrayContaining("-nested", "+dimension"));
    }

    public void testNestedScopeWithAllFieldsAsksForAllFieldsOnly() {
        resolveMain(IndexResolver.ALL_FIELDS, false, true);
        assertThat(captured.get().fields(), is(new String[] { "*" }));
        assertThat(captured.get().filters(), is(new String[0]));
    }

    public void testFlatIndicesFollowNestedScope() {
        resolveFlat(NESTED_NAMES, false, true);
        assertThat(captured.get().filters(), is(new String[0]));
        assertThat(Set.of(captured.get().fields()), equalTo(NESTED_NAMES));
        resolveFlat(NESTED_NAMES, false, false);
        assertThat(captured.get().filters(), arrayContaining("-nested"));
        resolveFlat(NESTED_NAMES, true, true);
        assertThat(captured.get().filters(), arrayContaining("+dimension"));
        resolveFlat(NESTED_NAMES, true, false);
        assertThat(captured.get().filters(), arrayContaining("-nested", "+dimension"));
    }

    public void testLookupIndicesKeepTheNestedFilterWhateverTheQueryNames() {
        resolver.resolveLookupIndices("lookup", NESTED_NAMES, TransportVersion.current(), ignoreResult());
        assertThat(captured.get().filters(), arrayContaining("-nested"));
        resolver.resolveLookupIndices("lookup", NESTED_NAMES, true, TransportVersion.current(), ignoreResult());
        assertThat(captured.get().filters(), arrayContaining("-nested"));
    }

    public void testExemplarDataStreamsKeepTheNestedFilter() {
        resolver.resolveExemplarDataStreamsVersioned("exemplars", null, TransportVersion.current(), ignoreResult());
        assertThat(captured.get().filters(), arrayContaining("-nested"));
    }

    public void testNestedExpandSetsNestedScopeWithoutAddingAFieldName() {
        assumeTrue("NESTED_EXPAND is snapshot-only", Build.current().isSnapshot());
        PreAnalysisResult result = resolveFieldNames("FROM ne | NESTED_EXPAND items | KEEP items.sku");
        assertThat(result.nestedScope(), is(true));
        assertThat(result.fieldNames(), hasItems("items", "items.*", "items.sku"));
        assertThat(result.fieldNames(), everyItem(not(containsString("\u0000"))));
    }

    public void testNestedExpandOverAllFieldsKeepsAllFields() {
        assumeTrue("NESTED_EXPAND is snapshot-only", Build.current().isSnapshot());
        PreAnalysisResult result = resolveFieldNames("FROM ne | NESTED_EXPAND items");
        assertThat(result.nestedScope(), is(true));
        assertThat(result.fieldNames(), equalTo(IndexResolver.ALL_FIELDS));
    }

    public void testNestedExpandCombinedWithLookupJoinSetsNestedScope() {
        assumeTrue("NESTED_EXPAND is snapshot-only", Build.current().isSnapshot());
        PreAnalysisResult result = resolveFieldNames("FROM ne | NESTED_EXPAND items | LOOKUP JOIN lk ON items.sku | KEEP items.sku, tag");
        assertThat(result.nestedScope(), is(true));
        assertThat(result.fieldNames(), not(equalTo(IndexResolver.ALL_FIELDS)));
    }

    public void testSnapshotQueryReadsNestedLeaves() {
        PreAnalysisResult result = resolveFieldNames("FROM ne | KEEP items.sku");
        assertThat(result.nestedScope(), is(Build.current().isSnapshot()));
    }

    public void testRequestFilterThatRequiresAllFieldsKeepsNestedScope() {
        PreAnalysisResult result = new PreAnalysisResult(Set.of("items.sku"), Set.of(), true, Set.of());
        PreAnalysisResult widened = EsqlSession.addFilterFieldNames(result, new MultiMatchQueryBuilder("a", "b"), EsqlTestUtils.TEST_CFG);
        assertThat(widened.fieldNames(), equalTo(IndexResolver.ALL_FIELDS));
        assertThat(widened.nestedScope(), is(true));
    }

    public void testRequestFilterFieldsKeepNestedScope() {
        PreAnalysisResult result = new PreAnalysisResult(Set.of("items.sku"), Set.of(), true, Set.of());
        PreAnalysisResult widened = EsqlSession.addFilterFieldNames(
            result,
            QueryBuilders.termQuery("region", "eu"),
            EsqlTestUtils.TEST_CFG
        );
        assertThat(widened.fieldNames(), containsInAnyOrder("items.sku", "region"));
        assertThat(widened.nestedScope(), is(true));
    }

    private static <T> ActionListener<T> ignoreResult() {
        return ActionListener.wrap(r -> {}, e -> {});
    }

    private static PreAnalysisResult resolveFieldNames(String query) {
        return FieldNameUtils.resolveFieldNames(InSubqueryResolver.resolve(TEST_PARSER.parseQuery(query)), false, false);
    }

    private void resolveMain(Set<String> fieldNames, boolean includeAllDimensions, boolean nestedScope) {
        resolver.resolveMainIndicesVersioned(
            "ne",
            fieldNames,
            null,
            includeAllDimensions,
            nestedScope,
            TransportVersion.current(),
            false,
            false,
            false,
            false,
            false,
            false,
            null,
            (options, expressions, returnLocalAll) -> Map.of(),
            null,
            ignoreResult()
        );
    }

    private void resolveFlat(Set<String> fieldNames, boolean includeAllDimensions, boolean nestedScope) {
        resolver.resolveFlatIndicesVersioned(
            false,
            "ne",
            null,
            fieldNames,
            null,
            includeAllDimensions,
            nestedScope,
            TransportVersion.current(),
            false,
            false,
            false,
            false,
            false,
            false,
            null,
            null,
            IndexViewOrigins.EMPTY,
            ignoreResult()
        );
    }
}
