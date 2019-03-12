/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.watcher.WatcherState;
import org.elasticsearch.xpack.core.watcher.client.WatcherClient;
import org.elasticsearch.xpack.core.watcher.execution.TriggeredWatchStoreField;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsResponse;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.clientWithOrigin;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/x-pack-elasticsearch/issues/3732")
public class IndexUpgradeWatcherIT extends IndexUpgradeIntegTestCase {

    @Before
    public void resetLicensing() throws Exception {
        updateLicensing(randomFrom("trial", "platinum", "gold"));
    }

    @After
    public void stopWatcher() throws Exception {
        Client client = clientWithOrigin(internalCluster().getInstance(Client.class, internalCluster().getMasterName()), WATCHER_ORIGIN);
        WatcherClient watcherClient = new WatcherClient(client);
        if (allNodesWithState(WatcherState.STOPPED, watcherClient) == false) {
            assertAcked(watcherClient.prepareWatchService().stop().get());
        }
        ensureWatcherStopped(watcherClient);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        // we need to enable watcher, as otherwise we cannot call the watcher stats API in the upgrade code
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), true);
        return settings.build();
    }

    public void testPreWatchesUpgrade() throws Exception {
        // use the internal client from the master, instead of client(), so we dont have to deal with remote transport exceptions
        // and it works like the real implementation
        Client client = clientWithOrigin(internalCluster().getInstance(Client.class, internalCluster().getMasterName()), WATCHER_ORIGIN);
        Settings templateSettings = Settings.builder().put("index.number_of_shards", 2).build();
        WatcherClient watcherClient = new WatcherClient(client);

        // create legacy watches template or make sure it is deleted
        if (randomBoolean()) {
            assertAcked(client().admin().indices().preparePutTemplate("watches")
                    .setSettings(templateSettings).setTemplate(".watches*")
                    .get());
        } else {
            try {
                client().admin().indices().prepareDeleteTemplate("watches").get();
            } catch (IndexTemplateMissingException e) {}
        }

        // create old watch history template
        if (randomBoolean()) {
            assertAcked(client().admin().indices().preparePutTemplate("watch_history_foo")
                    .setSettings(templateSettings).setTemplate("watch_history-*")
                    .get());
        }

        boolean expectWatcherToBeRestartedByUpgrade = randomBoolean();
        logger.info("watcher stopped manually before upgrade [{}]", expectWatcherToBeRestartedByUpgrade);
        // if we expect watcher to be restarted we should make sure it is already started, otherwise it wont be
        ensureWatcherIsInCorrectState(watcherClient, expectWatcherToBeRestartedByUpgrade);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exception = new AtomicReference<>();
        AtomicReference<Boolean> listenerResult = new AtomicReference<>();
        ActionListener<Boolean> listener = createLatchListener(latch, listenerResult, exception);

        Upgrade.preWatchesIndexUpgrade(client, getClusterState(), listener);

        assertThat("Latch was not counted down", latch.await(10, TimeUnit.SECONDS), is(true));
        assertThat(exception.get(), is(nullValue()));
        assertThat(listenerResult, is(notNullValue()));
        String msg = String.format(Locale.ROOT, "Expected expectWatcherToBeRestartedByUpgrade [%s], but was listener result [%s]",
                expectWatcherToBeRestartedByUpgrade, listenerResult.get());
        assertThat(msg, listenerResult.get(), is(expectWatcherToBeRestartedByUpgrade));

        // ensure old index templates are gone, new ones are created
        List<String> templateNames = getTemplateNames();
        assertThat(templateNames, not(hasItem(startsWith("watch_history"))));
        assertThat(templateNames, not(hasItem("watches")));
        assertThat(templateNames, hasItem(".watches"));

        // last let's be sure that the watcher index template registry does not add back any template by accident with the current state
        triggerClusterStateEvent();

        List<String> templateNamesAfterClusterChangedEvent = getTemplateNames();
        assertThat(templateNamesAfterClusterChangedEvent, not(hasItem(startsWith("watch_history"))));
        assertThat(templateNamesAfterClusterChangedEvent, not(hasItem("watches")));
        assertThat(templateNamesAfterClusterChangedEvent, hasItem(".watches"));

        CountDownLatch postUpgradeLatch = new CountDownLatch(1);
        Upgrade.postWatchesIndexUpgrade(client, listenerResult.get(), ActionListener.wrap(
                r -> postUpgradeLatch.countDown(),
                e -> postUpgradeLatch.countDown()
        ));
        assertThat("Latch was not counted down", postUpgradeLatch.await(10, TimeUnit.SECONDS), is(true));

        boolean isWatcherStopped = new WatcherClient(client).prepareWatcherStats().get().watcherMetaData().manuallyStopped();
        assertThat(isWatcherStopped, is(expectWatcherToBeRestartedByUpgrade == false));
    }

    private ClusterState getClusterState() {
        ClusterService masterTokenService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        return masterTokenService.state();
    }

    private void ensureWatcherIsInCorrectState(WatcherClient watcherClient,
                                               boolean expectWatcherToBeRestartedByUpgrade) throws Exception {
        if (expectWatcherToBeRestartedByUpgrade) {
            if (allNodesWithState(WatcherState.STARTED, watcherClient) == false) {
                assertAcked(watcherClient.prepareWatchService().start().get());
            }
            ensureWatcherStarted(watcherClient);
        } else {
            if (allNodesWithState(WatcherState.STOPPED, watcherClient) == false) {
                assertAcked(watcherClient.prepareWatchService().stop().get());
            }
            ensureWatcherStopped(watcherClient);
        }
    }

    public void testPreTriggeredWatchesUpgrade() throws Exception {
        // use the internal client from the master, instead of client(), so we dont have to deal with remote transport exceptions
        // and it works like the real implementation
        Client client = clientWithOrigin(internalCluster().getInstance(Client.class, internalCluster().getMasterName()), WATCHER_ORIGIN);
        WatcherClient watcherClient = new WatcherClient(client);

        Settings templateSettings = Settings.builder().put("index.number_of_shards", 2).build();
        // create legacy triggered watch template
        if (randomBoolean()) {
            assertAcked(client().admin().indices().preparePutTemplate("triggered_watches")
                    .setSettings(templateSettings).setTemplate(".triggered_watches*")
                    .get());
        } else {
            try {
                client().admin().indices().prepareDeleteTemplate("triggered_watches").get();
            } catch (IndexTemplateMissingException e) {}
        }

        boolean expectWatcherToBeRestartedByUpgrade = randomBoolean();
        logger.info("watcher stopped manually before upgrade [{}]", expectWatcherToBeRestartedByUpgrade);
        // if we expect watcher to be restarted we should make sure it is already started, otherwise it wont be
        ensureWatcherIsInCorrectState(watcherClient, expectWatcherToBeRestartedByUpgrade);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exception = new AtomicReference<>();
        AtomicReference<Boolean> listenerResult = new AtomicReference<>();
        ActionListener<Boolean> listener = createLatchListener(latch, listenerResult, exception);

        Upgrade.preTriggeredWatchesIndexUpgrade(client, getClusterState(), listener);

        assertThat("Latch was not counted down", latch.await(10, TimeUnit.SECONDS), is(true));
        assertThat(exception.get(), is(nullValue()));
        assertThat(listenerResult, is(notNullValue()));
        String msg = String.format(Locale.ROOT, "Expected expectWatcherToBeRestartedByUpgrade [%s], but was listener result [%s]",
                expectWatcherToBeRestartedByUpgrade, listenerResult.get());
        assertThat(msg, listenerResult.get(), is(expectWatcherToBeRestartedByUpgrade));

        // ensure old index templates are gone, new ones are created
        List<String> templateNames = getTemplateNames();
        assertThat(templateNames, not(hasItem("triggered_watches")));
        assertThat(templateNames, hasItem(".triggered_watches"));

        // last let's be sure that the watcher index template registry does not add back any template by accident with the current state
        triggerClusterStateEvent();
        List<String> templateNamesAfterClusterChangedEvent = getTemplateNames();
        assertThat(templateNamesAfterClusterChangedEvent, not(hasItem("triggered_watches")));
        assertThat(templateNamesAfterClusterChangedEvent, hasItem(".triggered_watches"));

        CountDownLatch postUpgradeLatch = new CountDownLatch(1);
        Upgrade.postWatchesIndexUpgrade(client, listenerResult.get(), ActionListener.wrap(
                r -> postUpgradeLatch.countDown(),
                e -> postUpgradeLatch.countDown()
        ));
        assertThat("Latch was not counted down", postUpgradeLatch.await(10, TimeUnit.SECONDS), is(true));

        boolean isWatcherStopped = new WatcherClient(client).prepareWatcherStats().get().watcherMetaData().manuallyStopped();
        assertThat(isWatcherStopped, is(expectWatcherToBeRestartedByUpgrade == false));
    }

    public void testThatBothWatcherUpgradesShouldStartWatcherAtTheEnd() throws Exception {
        // create two old indices with no index.format
        for (String index : new String[]{Watch.INDEX, TriggeredWatchStoreField.INDEX_NAME}) {
            client().admin().indices().prepareCreate(index).get();
        }
        ensureGreen(Watch.INDEX, TriggeredWatchStoreField.INDEX_NAME);

        Client client = clientWithOrigin(internalCluster().getInstance(Client.class, internalCluster().getMasterName()), WATCHER_ORIGIN);
        WatcherClient watcherClient = new WatcherClient(client);

        // most users will have watcher started, so let's make sure we have as well
        watcherClient.prepareWatchService().start().get();
        ensureWatcherStarted(watcherClient);

        // upgrade the first index
        {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Exception> exception = new AtomicReference<>();
            AtomicReference<Boolean> listenerResult = new AtomicReference<>();
            ActionListener<Boolean> listener = createLatchListener(latch, listenerResult, exception);
            logger.info("running Upgrade.preTriggeredWatchesIndexUpgrade()");
            Upgrade.preTriggeredWatchesIndexUpgrade(client, getClusterState(), listener);
            assertThat("Latch was not counted down", latch.await(10, TimeUnit.SECONDS), is(true));
            assertThat(exception.get(), is(nullValue()));
            assertThat(listenerResult.get(), is(true));

            CountDownLatch postUpgradeLatch = new CountDownLatch(1);
            logger.info("running Upgrade.postWatchesIndexUpgrade()");
            Upgrade.postWatchesIndexUpgrade(client, listenerResult.get(), ActionListener.wrap(
                    r -> postUpgradeLatch.countDown(),
                    e -> postUpgradeLatch.countDown()
            ));
            assertThat("Latch was not counted down", postUpgradeLatch.await(10, TimeUnit.SECONDS), is(true));
        }

        // only one index updated yet, should not be set to manually stopped
        {
            WatcherStatsResponse watcherStatsResponse = watcherClient.prepareWatcherStats().get();
            assertThat(watcherStatsResponse.watcherMetaData().manuallyStopped(), is(false));
            ensureGreen(Watch.INDEX, TriggeredWatchStoreField.INDEX_NAME);
            ensureWatcherStarted(watcherClient);
        }

        // upgrade the second index
        {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Exception> exception = new AtomicReference<>();
            AtomicReference<Boolean> listenerResult = new AtomicReference<>();
            ActionListener<Boolean> listener = createLatchListener(latch, listenerResult, exception);
            logger.info("running Upgrade.preWatchesIndexUpgrade()");
            Upgrade.preWatchesIndexUpgrade(client, getClusterState(), listener);
            assertThat("Latch was not counted down", latch.await(10, TimeUnit.SECONDS), is(true));
            assertThat(exception.get(), is(nullValue()));
            assertThat(listenerResult.get(), is(true));

            CountDownLatch postUpgradeLatch = new CountDownLatch(1);
            logger.info("running Upgrade.postWatchesIndexUpgrade()");
            Upgrade.postWatchesIndexUpgrade(client, listenerResult.get(), ActionListener.wrap(
                    r -> postUpgradeLatch.countDown(),
                    e -> postUpgradeLatch.countDown()
            ));
            assertThat("Latch was not counted down", postUpgradeLatch.await(10, TimeUnit.SECONDS), is(true));
        }

        // only one index updated yet, should not be set to manually stopped, but we should also have started
        {
            WatcherStatsResponse watcherStatsResponse = watcherClient.prepareWatcherStats().get();
            assertThat(watcherStatsResponse.watcherMetaData().manuallyStopped(), is(false));
            ensureGreen(Watch.INDEX, TriggeredWatchStoreField.INDEX_NAME);
            ensureWatcherStarted(watcherClient);
        }
    }

    private ActionListener<Boolean> createLatchListener(CountDownLatch latch,
                                                        AtomicReference<Boolean> listenerResult,
                                                        AtomicReference<Exception> exception) {
        return ActionListener.wrap(
                r -> {
                    listenerResult.set(r);
                    latch.countDown();
                },
                e -> {
                    latch.countDown();
                    exception.set(e);
                });
    }

    private void triggerClusterStateEvent() {
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        Client client = clientWithOrigin(internalCluster().getInstance(Client.class, internalCluster().getMasterName()), WATCHER_ORIGIN);
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, internalCluster().getMasterName());
        WatcherIndexTemplateRegistry registry =
                new WatcherIndexTemplateRegistry(clusterService, threadPool, client);

        ClusterState state = clusterService.state();
        ClusterChangedEvent event = new ClusterChangedEvent("whatever", state, state);
        registry.clusterChanged(event);
    }

    private List<String> getTemplateNames() {
        GetIndexTemplatesResponse templatesResponse = client().admin().indices().prepareGetTemplates().get();
        return templatesResponse.getIndexTemplates().stream()
                .map(IndexTemplateMetaData::getName)
                .collect(Collectors.toList());
    }

    private void ensureWatcherStarted(WatcherClient watcherClient) throws Exception {
        assertBusy(() -> assertThat(allNodesWithState(WatcherState.STARTED, watcherClient), is(true)));
    }

    private void ensureWatcherStopped(WatcherClient watcherClient) throws Exception {
        assertBusy(() -> assertThat(allNodesWithState(WatcherState.STOPPED, watcherClient), is(true)));
    }

    private boolean allNodesWithState(WatcherState expectedState, WatcherClient watcherClient) {
        WatcherStatsResponse stats = watcherClient.prepareWatcherStats().get();
        List<Tuple<DiscoveryNode, WatcherState>> stateByNode = stats.getNodes().stream()
                .map(node -> Tuple.tuple(node.getNode(), node.getWatcherState()))
                .collect(Collectors.toList());
        logger.info("expecting state [{}], current watcher states per node: [{}]", expectedState, stateByNode);
        return stateByNode.stream().map(Tuple::v2).allMatch(state -> state == expectedState);
    }
}