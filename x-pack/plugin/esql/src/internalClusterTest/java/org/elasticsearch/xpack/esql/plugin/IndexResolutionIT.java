/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class IndexResolutionIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), DataStreamsPlugin.class);
    }

    public void testResolvesConcreteIndex() {
        assertAcked(client().admin().indices().prepareCreate("index-1"));
        indexRandom(true, "index-1", 1);

        try (var response = run(syncEsqlQueryRequest().query("FROM index-1"))) {
            assertOk(response);
        }
    }

    public void testResolvesAlias() {
        assertAcked(client().admin().indices().prepareCreate("index-1"));
        indexRandom(true, "index-1", 1);
        assertAcked(client().admin().indices().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("index-1", "alias-1"));

        try (var response = run(syncEsqlQueryRequest().query("FROM alias-1"))) {
            assertOk(response);
        }
    }

    public void testResolvesDataStream() {
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request("data-stream-1-template").indexTemplate(
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of("data-stream-1*"))
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                        .build()
                )
            )
        );
        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "data-stream-1")
            )
        );

        try (var response = run(syncEsqlQueryRequest().query("FROM data-stream-1"))) {
            assertOk(response);
        }
    }

    public void testResolvesPattern() {
        assertAcked(client().admin().indices().prepareCreate("index-1"));
        indexRandom(true, "index-1", 1);
        assertAcked(client().admin().indices().prepareCreate("index-2"));
        indexRandom(true, "index-2", 1);

        try (var response = run(syncEsqlQueryRequest().query("FROM index-*"))) {
            assertOk(response);
        }
    }

    public void testResolvesExclusionPattern() {
        assertAcked(client().admin().indices().prepareCreate("index-1"));
        indexRandom(true, "index-1", 1);
        assertAcked(client().admin().indices().prepareCreate("index-2"));
        indexRandom(true, "index-2", 1);

        try (var response = run(syncEsqlQueryRequest().query("FROM index*,-index-2 METADATA _index"))) {
            assertOk(response);
            assertResultConcreteIndices(response, "index-1");// excludes concrete index from pattern
        }
        try (var response = run(syncEsqlQueryRequest().query("FROM index*,-*2 METADATA _index"))) {
            assertOk(response);
            assertResultConcreteIndices(response, "index-1");// excludes pattern from pattern
        }
        expectThrows(
            VerificationException.class,
            containsString("Unknown index [index-*,-*]"),
            () -> run(syncEsqlQueryRequest().query("FROM index-*,-* METADATA _index")) // exclude all resolves to empty
        );
    }

    public void testDoesNotResolveEmptyPattern() {
        assertAcked(client().admin().indices().prepareCreate("data"));
        indexRandom(true, "data", 1);

        expectThrows(
            VerificationException.class,
            containsString("Unknown index [index-*]"),
            () -> run(syncEsqlQueryRequest().query("FROM index-* METADATA _index"))
        );

        try (var response = run(syncEsqlQueryRequest().query("FROM data,index-* METADATA _index"))) {
            assertOk(response);
            assertResultConcreteIndices(response, "data");
        }
    }

    public void testDoesNotResolveUnknownIndex() {
        expectThrows(
            VerificationException.class,
            containsString("Unknown index [no-such-index]"),
            () -> run(syncEsqlQueryRequest().query("FROM no-such-index"))
        );
    }

    public void testDoesNotResolveClosedIndex() {
        assertAcked(client().admin().indices().prepareCreate("index-1"));
        indexRandom(true, "index-1", 1);
        // Create index only waits for primary/indexing shard to be assigned.
        // This is enough to index and search documents, however all shards (including replicas) must be assigned before close.
        ensureGreen("index-1");
        assertAcked(client().admin().indices().prepareClose("index-1"));
        assertAcked(client().admin().indices().prepareCreate("index-2"));
        indexRandom(true, "index-2", 1);

        expectThrows(
            ClusterBlockException.class,
            containsString("index [index-1] blocked by: [FORBIDDEN/4/index closed]"),
            () -> run(syncEsqlQueryRequest().query("FROM index-1"))
        );
        expectThrows(
            ClusterBlockException.class,
            containsString("index [index-1] blocked by: [FORBIDDEN/4/index closed]"),
            () -> run(syncEsqlQueryRequest().query("FROM index-1,index-2"))
        );
        try (var response = run(syncEsqlQueryRequest().query("FROM index-* METADATA _index"))) {
            assertOk(response);
            assertResultConcreteIndices(response, "index-2"); // only open index-2 matches
        }
    }

    public void testHiddenIndices() {
        assertAcked(client().admin().indices().prepareCreate("regular-index-1"));
        indexRandom(true, "regular-index-1", 1);
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(".hidden-index-1")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true))
        );
        indexRandom(true, ".hidden-index-1", 1);

        try (var response = run(syncEsqlQueryRequest().query("FROM .hidden-index-1 METADATA _index"))) {
            assertOk(response);
            assertResultConcreteIndices(response, ".hidden-index-1");
        }
        try (var response = run(syncEsqlQueryRequest().query("FROM *-index-1 METADATA _index"))) {
            assertOk(response);
            assertResultConcreteIndices(response, "regular-index-1"); // only non-hidden index matches when specifying pattern
        }
        try (var response = run(syncEsqlQueryRequest().query("FROM .hidden-* METADATA _index"))) {
            assertOk(response);
            assertResultConcreteIndices(response, ".hidden-index-1"); // hidden indices do match when specifying hidden/dot pattern
        }
    }

    public void testUnavailableIndex() {
        assertAcked(client().admin().indices().prepareCreate("available-index-1"));
        indexRandom(true, "available-index-1", 1);
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("unavailable-index-1")
                .setSettings(Settings.builder().put("index.routing.allocation.require._name", UUID.randomUUID().toString()))
                .setWaitForActiveShards(ActiveShardCount.NONE)
        );

        expectThrows(
            NoShardAvailableActionException.class,
            containsString("index [unavailable-index-1] has no active shard copy"),
            () -> run(syncEsqlQueryRequest().query("FROM unavailable-index-1"))
        );
        expectThrows(
            NoShardAvailableActionException.class,
            containsString("index [unavailable-index-1] has no active shard copy"),
            () -> run(syncEsqlQueryRequest().query("FROM unavailable-index-1,available-index-1"))
        );
        expectThrows(
            NoShardAvailableActionException.class,
            containsString("index [unavailable-index-1] has no active shard copy"),
            () -> run(syncEsqlQueryRequest().query("FROM *-index-1"))
        );
        expectThrows(
            NoShardAvailableActionException.class,
            containsString("index [unavailable-index-1] has no active shard copy"),
            () -> run(syncEsqlQueryRequest().query("FROM unavailable-index-1").allowPartialResults(true))
        );
    }

    public void testPartialResolution() {
        assertAcked(client().admin().indices().prepareCreate("index-1"));
        indexRandom(true, "index-1", 1);

        expectThrows(
            IndexNotFoundException.class,
            equalTo("no such index [nonexisting-1]"), // fails when present index is non-empty
            () -> run(syncEsqlQueryRequest().query("FROM index-1,nonexisting-1"))
        );
        expectThrows(
            IndexNotFoundException.class,
            equalTo("no such index [nonexisting-1]"), // fails when present index is non-empty even if allow_partial=true
            () -> run(syncEsqlQueryRequest().query("FROM index-1,nonexisting-1").allowPartialResults(true))
        );
        expectThrows(
            IndexNotFoundException.class,
            equalTo("no such index [nonexisting-1]"), // only the first missing index is reported
            () -> run(syncEsqlQueryRequest().query("FROM index-1,nonexisting-1,nonexisting-2"))
        );

        assertAcked(client().admin().indices().prepareCreate("index-2").setMapping("field", "type=keyword"));
        expectThrows(
            IndexNotFoundException.class,
            equalTo("no such index [nonexisting-1]"), // fails when present index has no documents and non-empty mapping
            () -> run(syncEsqlQueryRequest().query("FROM index-2,nonexisting-1"))
        );

        assertAcked(client().admin().indices().prepareCreate("index-3"));
        try (var response = run(syncEsqlQueryRequest().query("FROM index-3,nonexisting-1"))) {
            assertOk(response); // passes when the present index has no fields and no documents
        }
    }

    public void testResolutionWithFilter() {
        assertAcked(client().admin().indices().prepareCreate("data"));
        indexRandom(true, "data", 1);

        try (var response = run(syncEsqlQueryRequest().query("FROM data METADATA _index").filter(new MatchAllQueryBuilder()))) {
            assertOk(response);
            assertResultConcreteIndices(response, "data");
        }
        try (var response = run(syncEsqlQueryRequest().query("FROM data METADATA _index").filter(new MatchNoneQueryBuilder()))) {
            assertOk(response);
            assertResultConcreteIndices(response);
        }
    }

    public void testSubqueryResolution() {
        assumeTrue("Requires subqueries feature", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        assertAcked(client().admin().indices().prepareCreate("index-1"));
        indexRandom(true, "index-1", 1);
        assertAcked(client().admin().indices().prepareCreate("index-2"));
        indexRandom(true, "index-2", 1);

        try (var response = run(syncEsqlQueryRequest().query("FROM (FROM index-1 METADATA _index), (FROM index-2 METADATA _index)"))) {
            assertOk(response);
            assertResultConcreteIndices(response, "index-1", "index-2");
        }
    }

    private static void assertOk(EsqlQueryResponse response) {
        assertThat(response.isPartial(), equalTo(false));
    }

    private static void assertResultConcreteIndices(EsqlQueryResponse response, Object... indices) {
        var indexColumn = findIndexColumn(response);
        assertThat(() -> response.column(indexColumn), containsInAnyOrder(indices));
    }

    private static int findIndexColumn(EsqlQueryResponse response) {
        for (int c = 0; c < response.columns().size(); c++) {
            if (Objects.equals(response.columns().get(c).name(), MetadataAttribute.INDEX)) {
                return c;
            }
        }
        throw new AssertionError("no _index column found");
    }
}
