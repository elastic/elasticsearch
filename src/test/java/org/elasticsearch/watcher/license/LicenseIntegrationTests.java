/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.license;

import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicenseExpiredException;
import org.elasticsearch.license.plugin.core.LicensesClientService;
import org.elasticsearch.license.plugin.core.LicensesService;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.watcher.WatcherVersion;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.transport.actions.service.WatcherServiceResponse;
import org.elasticsearch.watcher.watch.Watch;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class LicenseIntegrationTests extends AbstractWatcherIntegrationTests {

    static final License DUMMY_LICENSE = License.builder()
            .feature(LicenseService.FEATURE_NAME)
            .expiryDate(System.currentTimeMillis())
            .issueDate(System.currentTimeMillis())
            .issuedTo("LicensingTests")
            .issuer("test")
            .maxNodes(Integer.MAX_VALUE)
            .signature("_signature")
            .type("test_license_for_watcher")
            .subscriptionType("all_is_good")
            .uid(String.valueOf(CHILD_JVM_ID) + System.identityHashCode(LicenseIntegrationTests.class))
            .build();

    @Override
    protected Class<? extends Plugin> licensePluginClass() {
        return MockLicensePlugin.class;
    }

    @Override
    protected boolean timeWarped() {
        return true;
    }

    @Override
    protected boolean checkWatcherRunningOnlyOnce() {
        return false;
    }

    @Test
    public void testEnableDisableBehaviour() throws Exception {

        // put watch API should work
        final String watchName = randomAsciiOfLength(10);
        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch(watchName).setSource(watchBuilder()
                .trigger(schedule(interval("1s")))
                .input(simpleInput())
                .condition(alwaysCondition())
                .addAction("_index", indexAction("idx", "type")))
                .execute().actionGet();

        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().scheduler().trigger(watchName);

        // waiting for the watch to be executed at least once... so we can ack it
        assertWatchWithMinimumPerformedActionsCount(watchName, 1, false);

        // ack watch API should work
        assertThat(watcherClient().prepareAckWatch(watchName).get().getStatus().ackStatus().state(), is(Watch.Status.AckStatus.State.ACKED));

        // get watch API should work
        assertThat(watcherClient().prepareGetWatch(watchName).get().getId(), is(watchName));

        // delete watch API should work
        assertThat(watcherClient().prepareDeleteWatch(watchName).get().isFound(), is(true));

        // watcher stats API should work
        assertThat(watcherClient().prepareWatcherStats().get().getVersion(), is(WatcherVersion.CURRENT));

        // watcher service API should work
        WatcherServiceResponse serviceResponse = watcherClient().prepareWatchService().restart().get();
        assertThat(serviceResponse.isAcknowledged(), is(true));

        ensureWatcherStarted();

        // lets put back the watch and so we can test it when the license is disabled
        putWatchResponse = watcherClient().preparePutWatch(watchName).setSource(watchBuilder()
                .trigger(schedule(interval("10s")))
                .input(simpleInput())
                .condition(alwaysCondition())
                .addAction("_index", indexAction("idx", "type")))
                .execute().actionGet();

        assertThat(putWatchResponse.isCreated(), is(true));

        flush();

        final long docCountBeforeDisable = docCount("idx", "type", matchAllQuery());
        assertThat(docCountBeforeDisable, is(1L));

        final long recordCountBeforeDisable = historyRecordsCount(watchName);
        assertThat(recordCountBeforeDisable, is(1L));

        final long executedBeforeDisable = findNumberOfPerformedActions(watchName);
        assertThat(executedBeforeDisable, is(1L));

        disableLicensing();


        //=====
        // first lets verify that when the license is disabled and the watch is triggered, it is executed,
        // the history record is written for it, but it's throttled and its actions are not executed
        //=====

        // trigger the watch.. should execute the watch but not its action
        // we need to move the clock so the watch_record id will be unique
        timeWarp().clock().fastForwardSeconds(10);
        timeWarp().scheduler().trigger(watchName);

        // lets wait until we have another history record
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(historyRecordsCount(watchName), greaterThan(recordCountBeforeDisable));
            }
        });

        // ensure that the number of executed records stayed the same
        assertThat(findNumberOfPerformedActions(watchName), equalTo(executedBeforeDisable));

        // while the execution count grows, the number of documents indexed by the action stays the same
        // as with the license disabled, the actions are not executed
        assertThat(docCount("idx", "type", matchAllQuery()), is(docCountBeforeDisable));

        // and last... lets verify that we have throttled watches due to license expiration
        long throttledCount = docCount(HistoryStore.INDEX_PREFIX + "*", HistoryStore.DOC_TYPE, filteredQuery(
                matchQuery("watch_execution.throttle_reason", "watcher license expired"),
                termFilter("watch_execution.throttled", true)));
        assertThat(throttledCount, is(1L));


        //=====
        // now... lets verify that all the watcher APIs are blocked when the license is disabled
        //=====

        try {
            watcherClient().preparePutWatch(watchName).setSource(watchBuilder()
                    .trigger(schedule(interval("1s")))
                    .input(simpleInput())
                    .condition(alwaysCondition())
                    .addAction("_index", indexAction("idx", "type")))
                    .execute().actionGet();
            fail("put watch API should NOT work when license is disabled");
        } catch (LicenseExpiredException lee) {
            assertThat(lee.feature(), is(LicenseService.FEATURE_NAME));
            assertThat(lee.status(), is(RestStatus.UNAUTHORIZED));
        }

        try {
            assertThat(watcherClient().prepareAckWatch(watchName).get().getStatus().ackStatus().state(), is(Watch.Status.AckStatus.State.ACKED));
            fail("ack watch APIshould NOT work when license is disabled");
        } catch (LicenseExpiredException lee) {
            assertThat(lee.feature(), is(LicenseService.FEATURE_NAME));
            assertThat(lee.status(), is(RestStatus.UNAUTHORIZED));
        }

        try {
            assertThat(watcherClient().prepareGetWatch(watchName).get().getId(), is(watchName));
            fail("get watch API should NOT work when license is disabled");
        } catch (LicenseExpiredException lee) {
            assertThat(lee.feature(), is(LicenseService.FEATURE_NAME));
            assertThat(lee.status(), is(RestStatus.UNAUTHORIZED));
        }

        try {
            assertThat(watcherClient().prepareDeleteWatch(watchName).get().isFound(), is(true));
            fail("delete watch API should NOT work when license is disabled");
        } catch (LicenseExpiredException lee) {
            assertThat(lee.feature(), is(LicenseService.FEATURE_NAME));
            assertThat(lee.status(), is(RestStatus.UNAUTHORIZED));
        }

        // watcher stats should work
        try {
            assertThat(watcherClient().prepareWatcherStats().get().getVersion(), is(WatcherVersion.CURRENT));
            fail("watcher stats API should NOT work when license is disabled");
        } catch (LicenseExpiredException lee) {
            assertThat(lee.feature(), is(LicenseService.FEATURE_NAME));
            assertThat(lee.status(), is(RestStatus.UNAUTHORIZED));
        }

        try {
            assertThat(watcherClient().prepareWatchService().restart().get().isAcknowledged(), is(true));
            fail("watcher service API should NOT work when license is disabled");
        } catch (LicenseExpiredException lee) {
            assertThat(lee.feature(), is(LicenseService.FEATURE_NAME));
            assertThat(lee.status(), is(RestStatus.UNAUTHORIZED));
        }

        enableLicensing();

        // put watch API should work
        putWatchResponse = watcherClient().preparePutWatch(watchName).setSource(watchBuilder()
                .trigger(schedule(interval("1s")))
                .input(simpleInput())
                .condition(alwaysCondition())
                .addAction("_index", indexAction("idx", "type")))
                .execute().actionGet();

        assertThat(putWatchResponse, notNullValue());

        // we need to move the clock so the watch_record id will be unique
        timeWarp().clock().fastForwardSeconds(10);
        timeWarp().scheduler().trigger(watchName);

        // waiting for the watch to be executed at least once... so we can ack it
        assertWatchWithMinimumPerformedActionsCount(watchName, 1, false);
        assertBusy(new Runnable() {
            @Override
            public void run() {
                Map<String, Object> source = watcherClient().prepareGetWatch(watchName).get().getSourceAsMap();
                assertThat(XContentMapValues.extractValue("status.ack.state", source), is((Object) "ackable"));
            }
        });

        // ack watch API should work
        assertThat(watcherClient().prepareAckWatch(watchName).get().getStatus().ackStatus().state(), is(Watch.Status.AckStatus.State.ACKED));

        // get watch API should work
        assertThat(watcherClient().prepareGetWatch(watchName).get().getId(), is(watchName));

        // delete watch API should work
        assertThat(watcherClient().prepareDeleteWatch(watchName).get().isFound(), is(true));

        // watcher stats API should work
        assertThat(watcherClient().prepareWatcherStats().get().getVersion(), is(WatcherVersion.CURRENT));

        // watcher service API should work
        assertThat(watcherClient().prepareWatchService().stop().get().isAcknowledged(), is(true));
    }

    public static void disableLicensing() {
        for (MockLicenseService service : internalTestCluster().getInstances(MockLicenseService.class)) {
            service.disable();
        }
    }

    public static void enableLicensing() {
        for (MockLicenseService service : internalTestCluster().getInstances(MockLicenseService.class)) {
            service.enable();
        }
    }

    public static class MockLicensePlugin extends AbstractPlugin {

        public static final String NAME = "internal-test-licensing";

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public String description() {
            return name();
        }

        @Override
        public Collection<Class<? extends Module>> modules() {
            return ImmutableSet.<Class<? extends Module>>of(InternalLicenseModule.class);
        }
    }

    public static class InternalLicenseModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(MockLicenseService.class).asEagerSingleton();
            bind(LicensesClientService.class).to(MockLicenseService.class);
        }
    }

    public static class MockLicenseService extends AbstractComponent implements LicensesClientService {

        private final List<Listener> listeners = new ArrayList<>();

        @Inject
        public MockLicenseService(Settings settings) {
            super(settings);
            enable();
        }

        @Override
        public void register(String s, LicensesService.TrialLicenseOptions trialLicenseOptions, Collection<LicensesService.ExpirationCallback> collection, Listener listener) {
            listeners.add(listener);
            enable();
        }

        public void enable() {
            // enabled all listeners (incl. shield)
            for (Listener listener : listeners) {
                listener.onEnabled(DUMMY_LICENSE);
            }
        }

        public void disable() {
            // only disable watcher listener (we need shield to work)
            for (Listener listener : listeners) {
                if (listener instanceof LicenseService.InternalListener) {
                    listener.onDisabled(DUMMY_LICENSE);
                }
            }
        }
    }
}
