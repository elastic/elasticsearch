/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.async.AsyncStopRequest;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.AbstractCrossClusterTestCase.getDriverTasks;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.deleteAsyncId;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.startAsyncQuery;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.waitForCluster;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

// This tests if enrich after stop works correctly
public class CrossClusterAsyncEnrichStopIT extends AbstractEnrichBasedCrossClusterTestCase {

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected boolean tolerateErrorsWhenWipingEnrichPolicies() {
        // attempt to wipe will fail since some clusters are already closed
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(SimplePauseFieldPlugin.class);
        return plugins;
    }

    @Before
    public void resetPlugin() {
        SimplePauseFieldPlugin.resetPlugin();
    }

    /**
     * This tests that enrich and aggs work after stop. It works like this:
     * 1. We launch the async request
     * 2. c2 index has the pause field which will pause the query until we allow it to proceed
     * 3. We wait until c1 is done and then stop the async request
     * 4. We allow the query to proceed
     * 5. The result should contain the data from local and c1 and coordinator-side enrichments should happen
     */
    public void testEnrichAfterStop() throws Exception {
        setupEventsIndexWithPause("c2");
        String query = String.format(Locale.ROOT, """
            FROM *:events,events
            | eval ip= TO_STR(host)
            | %s
            | %s
            | eval const = coalesce(const, 1)
            | stats c = sum(const) by vendor
            | sort vendor
            """, enrichHosts(Enrich.Mode.COORDINATOR), enrichVendors(Enrich.Mode.COORDINATOR));

        // Start the async query
        final String asyncExecutionId = startAsyncQuery(client(), query, randomBoolean());
        SimplePauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS);

        // wait until c1 is done
        waitForCluster(client(), "c1", asyncExecutionId);
        waitForCluster(client(), LOCAL_CLUSTER, asyncExecutionId);
        // wait until remote reduce task starts on c2
        assertBusy(() -> {
            List<TaskInfo> tasks = getDriverTasks(client(REMOTE_CLUSTER_2));
            List<TaskInfo> reduceTasks = tasks.stream()
                .filter(t -> t.status() instanceof DriverStatus ds && ds.description().equals("remote_reduce"))
                .toList();
            assertThat(reduceTasks, not(empty()));
        });

        // Run the stop request
        var stopRequest = new AsyncStopRequest(asyncExecutionId);
        var stopAction = client().execute(EsqlAsyncStopAction.INSTANCE, stopRequest);
        // wait until remote reduce tasks are gone
        assertBusy(() -> {
            List<TaskInfo> tasks = getDriverTasks(client(REMOTE_CLUSTER_2));
            List<TaskInfo> reduceTasks = tasks.stream()
                .filter(t -> t.status() instanceof DriverStatus ds && ds.description().equals("remote_reduce"))
                .toList();
            assertThat(reduceTasks, empty());
        });

        // Allow the processing to proceed
        SimplePauseFieldPlugin.allowEmitting.countDown();

        try (EsqlQueryResponse resp = stopAction.actionGet(30, TimeUnit.SECONDS)) {
            // Compare this to CrossClustersEnrichIT.testEnrichTwiceThenAggs - the results from c2 will be absent
            // because we stopped it before processing the data
            assertThat(
                getValuesList(resp),
                equalTo(
                    List.of(
                        List.of(5L, "Apple"),
                        List.of(6L, "Microsoft"),
                        List.of(3L, "Redhat"),
                        List.of(2L, "Samsung"),
                        Arrays.asList(2L, (String) null)
                    )
                )
            );
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", "c1", "c2")));
        } finally {
            assertAcked(deleteAsyncId(client(), asyncExecutionId));
        }
    }

    private void setupEventsIndexWithPause(String clusterAlias) throws IOException {
        record Event(long timestamp, String user, String host) {}
        List<Event> events = List.of(
            new Event(1, "park", "192.168.1.25"),
            new Event(2, "akio", "192.168.1.5"),
            new Event(3, "park", "192.168.1.2"),
            new Event(4, "kevin", "192.168.1.3")
        );
        // Regular mapping
        var stdMapping = PutMappingRequest.simpleMapping("timestamp", "type=long", "user", "type=keyword", "host", "type=ip");
        Map<String, Object> mappingMap = XContentHelper.convertToMap(BytesReference.bytes(stdMapping), false, stdMapping.contentType())
            .v2();
        // Pause field mapping
        var mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("const");
            {
                mapping.field("type", "long");
                mapping.startObject("script").field("source", "").field("lang", "pause").endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        mapping.endObject();
        Map<String, Object> mappingMap2 = XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2();
        // Merge the two mappings
        mappingMap.putAll(mappingMap2);

        var client = client(clusterAlias);
        assertAcked(client.admin().indices().prepareDelete("events"));
        assertAcked(client.admin().indices().prepareCreate("events").setMapping(mappingMap));
        for (var e : events) {
            client.prepareIndex("events").setSource("timestamp", e.timestamp, "user", e.user, "host", e.host, "const", "1").get();
        }
        client.admin().indices().prepareRefresh("events").get();
    }
}
