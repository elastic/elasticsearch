/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.action.stats;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.watcher.WatcherState;
import org.elasticsearch.xpack.watcher.actions.ActionBuilders;
import org.elasticsearch.xpack.watcher.condition.ScriptCondition;
import org.elasticsearch.xpack.watcher.execution.ExecutionPhase;
import org.elasticsearch.xpack.watcher.execution.QueuedWatch;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionSnapshot;
import org.elasticsearch.xpack.watcher.input.InputBuilders;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.stats.WatcherStatsResponse;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.noneInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(scope = SUITE, numClientNodes = 0, transportClientRatio = 0,
        numDataNodes = 1, supportsDedicatedMasters = false)
public class WatchStatsTests extends AbstractWatcherIntegrationTestCase {

    private static CountDownLatch scriptStartedLatch = new CountDownLatch(1);
    private static CountDownLatch scriptCompletionLatch = new CountDownLatch(1);
    private static Logger logger = ESLoggerFactory.getLogger(WatchStatsTests.class);

    public static class LatchScriptPlugin extends MockScriptPlugin {

        private static final String NAME = "latch";

        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(NAME, p -> {
                scriptStartedLatch.countDown();
                try {
                    if (scriptCompletionLatch.await(10, TimeUnit.SECONDS) == false) {
                        logger.error("Script completion latch was not counted down after 10 seconds");
                    }
                } catch (InterruptedException e) {
                }
                return true;
            });
        }
    }

    static Script latchScript() {
        return new Script(ScriptType.INLINE, "mockscript", LatchScriptPlugin.NAME, Collections.emptyMap());
    }

    public void awaitScriptStartedExecution() throws InterruptedException {
        if (scriptStartedLatch.await(10, TimeUnit.SECONDS) == false) {
            throw new ElasticsearchException("Expected script to be called within 10 seconds, did not happen");
        }
    }

    @Override
    protected boolean timeWarped() {
        return false;
    }

    @Override
    protected boolean enableSecurity() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(LatchScriptPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                // So it is predictable how many slow watches we need to add to accumulate pending watches
                .put(EsExecutors.PROCESSORS_SETTING.getKey(), "1")
                .build();
    }

    @Before
    public void createLatches() {
        scriptStartedLatch = new CountDownLatch(1);
        scriptCompletionLatch = new CountDownLatch(1);
    }

    @After
    public void clearLatches() throws InterruptedException {
        scriptCompletionLatch.countDown();
        boolean countedDown = scriptCompletionLatch.await(10, TimeUnit.SECONDS);
        String msg = String.format(Locale.ROOT, "Script completion latch value is [%s], but should be 0", scriptCompletionLatch.getCount());
        assertThat(msg, countedDown, Matchers.is(true));
    }

    @TestLogging("org.elasticsearch.xpack.watcher.trigger.schedule.engine:TRACE,org.elasticsearch.xpack.scheduler:TRACE,org.elasticsearch" +
            ".xpack.watcher.execution:TRACE,org.elasticsearch.xpack.watcher.test:TRACE")
    public void testCurrentWatches() throws Exception {
        watcherClient().preparePutWatch("_id").setSource(watchBuilder()
                .trigger(schedule(interval("1s")))
                .input(InputBuilders.simpleInput("key", "value"))
                .condition(new ScriptCondition(latchScript()))
                .addAction("_action", ActionBuilders.loggingAction("some logging"))
        ).get();

        logger.info("Waiting for first script invocation");
        awaitScriptStartedExecution();
        logger.info("First script got executed, checking currently running watches");

        WatcherStatsResponse response = watcherClient().prepareWatcherStats().setIncludeCurrentWatches(true).get();
        boolean watcherStarted = response.getNodes().stream().anyMatch(node -> node.getWatcherState() == WatcherState.STARTED);
        assertThat(watcherStarted, is(true));

        assertThat(response.getWatchesCount(), equalTo(1L));
        assertThat(getQueuedWatches(response), hasSize(0));
        List<WatchExecutionSnapshot> snapshots = getSnapshots(response);
        assertThat(snapshots, notNullValue());
        assertThat(snapshots, hasSize(1));
        assertThat(snapshots.get(0).watchId(), equalTo("_id"));
        assertThat(snapshots.get(0).executionPhase(), equalTo(ExecutionPhase.CONDITION));
    }

    @TestLogging("org.elasticsearch.xpack.watcher.trigger.schedule.engine:TRACE,org.elasticsearch.xpack.scheduler:TRACE,org.elasticsearch" +
            ".xpack.watcher.execution:TRACE,org.elasticsearch.xpack.watcher.test:TRACE")
    public void testPendingWatches() throws Exception {
        // Add 5 slow watches and we should almost immediately see pending watches in the stats api
        for (int i = 0; i < 5; i++) {
            watcherClient().preparePutWatch("_id" + i).setSource(watchBuilder()
                    .trigger(schedule(interval("1s")))
                    .input(noneInput())
                    .condition(new ScriptCondition(latchScript()))
                    .addAction("_action", ActionBuilders.loggingAction("some logging"))
            ).get();
        }

        logger.info("Waiting for first script invocation");
        awaitScriptStartedExecution();
        // I know this still sucks, but it is still way faster than the older implementation
        logger.info("Sleeping 2.5 seconds to make sure a new round of watches is queued");
        Thread.sleep(2500);
        logger.info("Sleeping done, checking stats response");

        WatcherStatsResponse response = watcherClient().prepareWatcherStats().setIncludeQueuedWatches(true).get();
        boolean watcherStarted = response.getNodes().stream().allMatch(node -> node.getWatcherState() == WatcherState.STARTED);
        assertThat(watcherStarted, is(true));
        assertThat(response.getWatchesCount(), equalTo(5L));
        assertThat(getSnapshots(response), hasSize(0));
        assertThat(getQueuedWatches(response), hasSize(greaterThanOrEqualTo(5)));
        DateTime previous = null;
        for (QueuedWatch queuedWatch : getQueuedWatches(response)) {
            assertThat(queuedWatch.watchId(),
                    anyOf(equalTo("_id0"), equalTo("_id1"), equalTo("_id2"), equalTo("_id3"), equalTo("_id4")));
            if (previous != null) {
                // older pending watch should be on top:
                assertThat(previous.getMillis(), lessThanOrEqualTo(queuedWatch.executionTime().getMillis()));
            }
            previous = queuedWatch.executionTime();
        }
        logger.info("Pending watches test finished, now counting down latches");
    }

    private List<WatchExecutionSnapshot> getSnapshots(WatcherStatsResponse response) {
        List<WatchExecutionSnapshot> snapshots = new ArrayList<>();
        response.getNodes().stream()
                .filter(node -> node.getSnapshots() != null)
                .forEach(node -> snapshots.addAll(node.getSnapshots()));
        return snapshots;
    }

    private List<QueuedWatch> getQueuedWatches(WatcherStatsResponse response) {
        final List<QueuedWatch> queuedWatches = new ArrayList<>();
        response.getNodes().stream()
                .filter(node -> node.getQueuedWatches() != null)
                .forEach(node -> queuedWatches.addAll(node.getQueuedWatches()));
        return queuedWatches;
    }
}
