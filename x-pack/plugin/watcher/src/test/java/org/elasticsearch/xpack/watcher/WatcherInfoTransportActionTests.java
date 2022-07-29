/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.watcher.WatcherFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.WatcherField;
import org.elasticsearch.xpack.core.watcher.WatcherMetadata;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsResponse;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WatcherInfoTransportActionTests extends ESTestCase {

    private MockLicenseState licenseState;
    private Client client;

    @Before
    public void init() throws Exception {
        licenseState = mock(MockLicenseState.class);
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);
    }

    public void testAvailable() {
        WatcherInfoTransportAction featureSet = new WatcherInfoTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            Settings.EMPTY,
            licenseState
        );
        boolean available = randomBoolean();
        when(licenseState.isAllowed(WatcherField.WATCHER_FEATURE)).thenReturn(available);
        assertThat(featureSet.available(), is(available));
    }

    public void testEnabled() {
        boolean enabled = randomBoolean();
        Settings.Builder settings = Settings.builder();
        if (enabled) {
            if (randomBoolean()) {
                settings.put("xpack.watcher.enabled", enabled);
            }
        } else {
            settings.put("xpack.watcher.enabled", enabled);
        }
        WatcherInfoTransportAction featureSet = new WatcherInfoTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            settings.build(),
            licenseState
        );
        assertThat(featureSet.enabled(), is(enabled));
    }

    public void testUsageStats() throws Exception {
        doAnswer(mock -> {
            @SuppressWarnings("unchecked")
            ActionListener<WatcherStatsResponse> listener = (ActionListener<WatcherStatsResponse>) mock.getArguments()[2];

            List<WatcherStatsResponse.Node> nodes = new ArrayList<>();
            DiscoveryNode first = new DiscoveryNode("first", buildNewFakeTransportAddress(), Version.CURRENT);
            WatcherStatsResponse.Node firstNode = new WatcherStatsResponse.Node(first);
            Counters firstCounters = new Counters();
            firstCounters.inc("foo.foo", 1);
            firstCounters.inc("foo.bar.baz", 1);
            firstNode.setStats(firstCounters);
            nodes.add(firstNode);

            DiscoveryNode second = new DiscoveryNode("second", buildNewFakeTransportAddress(), Version.CURRENT);
            WatcherStatsResponse.Node secondNode = new WatcherStatsResponse.Node(second);
            Counters secondCounters = new Counters();
            secondCounters.inc("spam", 1);
            secondCounters.inc("foo.bar.baz", 4);
            secondNode.setStats(secondCounters);
            nodes.add(secondNode);

            listener.onResponse(
                new WatcherStatsResponse(new ClusterName("whatever"), new WatcherMetadata(false), nodes, Collections.emptyList())
            );
            return null;
        }).when(client).execute(eq(WatcherStatsAction.INSTANCE), any(), any());
        ClusterService clusterService = mock(ClusterService.class);
        final DiscoveryNode mockNode = mock(DiscoveryNode.class);
        when(mockNode.getId()).thenReturn("mocknode");
        when(clusterService.localNode()).thenReturn(mockNode);

        var usageAction = new WatcherUsageTransportAction(
            mock(TransportService.class),
            clusterService,
            null,
            mock(ActionFilters.class),
            null,
            Settings.EMPTY,
            licenseState,
            client
        );
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(mock(Task.class), null, null, future);
        WatcherFeatureSetUsage watcherUsage = (WatcherFeatureSetUsage) future.get().getUsage();
        assertThat(watcherUsage.stats().keySet(), containsInAnyOrder("foo", "spam"));
        long fooBarBaz = ObjectPath.eval("foo.bar.baz", watcherUsage.stats());
        assertThat(fooBarBaz, is(5L));
        long fooFoo = ObjectPath.eval("foo.foo", watcherUsage.stats());
        assertThat(fooFoo, is(1L));
        long spam = ObjectPath.eval("spam", watcherUsage.stats());
        assertThat(spam, is(1L));
        BytesStreamOutput out = new BytesStreamOutput();
        watcherUsage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new WatcherFeatureSetUsage(out.bytes().streamInput());

        for (XPackFeatureSet.Usage usage : Arrays.asList(watcherUsage, serializedUsage)) {
            XContentBuilder builder = jsonBuilder();
            usage.toXContent(builder, ToXContent.EMPTY_PARAMS);

            XContentSource source = new XContentSource(builder);
            assertThat(source.getValue("foo.bar.baz"), is(5));
            assertThat(source.getValue("spam"), is(1));
            assertThat(source.getValue("foo.foo"), is(1));

            assertThat(usage, instanceOf(WatcherFeatureSetUsage.class));
            WatcherFeatureSetUsage featureSetUsage = (WatcherFeatureSetUsage) usage;
            assertThat(featureSetUsage.stats().keySet(), containsInAnyOrder("foo", "spam"));
        }
    }
}
