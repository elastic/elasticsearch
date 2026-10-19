/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EnrichIT;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlQueryAction;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Calls {@link EnrichLookupService} directly with the target index that a physical plan received from another cluster
 * could carry, to check that the lookup only ever reads the concrete index of a local enrich policy.
 */
public class EnrichLookupIndexValidationIT extends AbstractEsqlIntegTestCase {

    private static final String POLICY_NAME = "users";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(
                CollectionUtils.appendToCopy(super.nodePlugins(), EnrichIT.LocalStateEnrich.class),
                IngestCommonPlugin.class
            ),
            ReindexPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .build();
    }

    @Before
    public void setupIndices() {
        createSingleShardIndex("users_source");
        client().prepareIndex("users_source").setSource("key", "k1", "value", "from-enrich").get();
        client().admin().indices().prepareRefresh("users_source").get();
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("users_source"), "key", List.of("value"));
        client().execute(PutEnrichPolicyAction.INSTANCE, new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, POLICY_NAME, policy))
            .actionGet();
        client().execute(ExecuteEnrichPolicyAction.INSTANCE, new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, POLICY_NAME))
            .actionGet();

        createSingleShardIndex("secrets");
        createSingleShardIndex(".enrich-unknown-1700000000000");
        for (String index : List.of("secrets", ".enrich-unknown-1700000000000")) {
            client().prepareIndex(index).setSource("key", "k1", "value", "secret").get();
            client().admin().indices().prepareRefresh(index).get();
        }
    }

    @After
    public void cleanup() {
        cluster().wipe(Set.of());
        client().execute(DeleteEnrichPolicyAction.INSTANCE, new DeleteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, POLICY_NAME))
            .actionGet();
    }

    public void testLookupOnEnrichPolicyIndexSucceeds() throws Exception {
        String enrichIndex = clusterService().state()
            .metadata()
            .getProject()
            .getIndicesLookup()
            .get(EnrichPolicy.getBaseName(POLICY_NAME))
            .getIndices()
            .getFirst()
            .getName();
        Page page = lookup(enrichIndex).get();
        try {
            BytesRefBlock values = page.getBlock(0);
            assertThat(values.getBytesRef(values.getFirstValueIndex(0), new BytesRef()).utf8ToString(), equalTo("from-enrich"));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testLookupOnRegularIndexIsRejected() {
        assertLookupRejected("secrets");
    }

    public void testLookupOnEnrichIndexWithoutPolicyIsRejected() {
        assertLookupRejected(".enrich-unknown-1700000000000");
    }

    private static void assertLookupRejected(String index) {
        PlainActionFuture<Page> future = lookup(index);
        ExecutionException e = expectThrows(ExecutionException.class, () -> future.get().releaseBlocks());
        assertThat(e.getCause(), instanceOf(ElasticsearchSecurityException.class));
        assertThat(((ElasticsearchSecurityException) e.getCause()).status(), is(RestStatus.FORBIDDEN));
        assertThat(e.getCause().getMessage(), containsString("[" + index + "]"));
    }

    private static void createSingleShardIndex(String index) {
        client().admin()
            .indices()
            .prepareCreate(index)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("key", "type=keyword", "value", "type=keyword")
            .get();
    }

    private static PlainActionFuture<Page> lookup(String index) {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        Block keys = blockFactory.newConstantBytesRefBlockWith(new BytesRef("k1"), 1);
        List<NamedExpression> extractFields = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            )
        );
        EnrichLookupService.Request request = new EnrichLookupService.Request(
            "test-session",
            index,
            DataType.KEYWORD,
            EnrichPolicy.MATCH_TYPE,
            "key",
            new Page(keys),
            extractFields,
            Source.EMPTY
        );
        CancellableTask parentTask = new CancellableTask(1, "test", "test", "test", null, Map.of());
        PlainActionFuture<Page> future = new PlainActionFuture<>();
        internalCluster().getInstance(TransportEsqlQueryAction.class)
            .enrichLookupService()
            .lookupAsync(request, parentTask, future.map(response -> {
                List<Page> pages = response.takePages();
                assertThat(pages, hasSize(1));
                return pages.getFirst();
            }));
        return future;
    }
}
