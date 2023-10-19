/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingCapacityAction;
import org.elasticsearch.xpack.autoscaling.action.PutAutoscalingPolicyAction;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ProactiveStorageIT extends AutoscalingStorageIntegTestCase {

    public void testScaleUp() throws IOException, InterruptedException {
        internalCluster().startMasterOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();
        final String policyName = "test";
        putAutoscalingPolicy(policyName, Settings.EMPTY);

        final String dsName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createDataStreamAndTemplate(dsName);
        final int rolloverCount = between(1, 5);
        for (int i = 0; i < rolloverCount; ++i) {
            indexRandom(
                true,
                false,
                IntStream.range(1, 100)
                    .mapToObj(
                        unused -> client().prepareIndex(dsName)
                            .setCreate(true)
                            .setSource("@timestamp", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(randomMillisUpToYear9999()))
                    )
                    .toArray(IndexRequestBuilder[]::new)
            );
            assertAcked(indicesAdmin().rolloverIndex(new RolloverRequest(dsName, null)).actionGet());
        }
        forceMerge();
        refresh();

        // just check it does not throw when not refreshed.
        capacity();

        IndicesStatsResponse stats = indicesAdmin().prepareStats(dsName).clear().setStore(true).get();
        long used = stats.getTotal().getStore().getSizeInBytes();
        long maxShardSize = Arrays.stream(stats.getShards()).mapToLong(s -> s.getStats().getStore().sizeInBytes()).max().orElseThrow();
        // As long as usage is above low watermark, we will trigger a proactive scale up, since the simulated shards have an in-sync
        // set and therefore allocating these do not skip the low watermark check in the disk threshold decider.
        // Fixing this simulation should be done as a separate effort, but we should still ensure that the low watermark is in effect
        // at least when replicas are involved.
        long enoughSpace = used + (randomBoolean()
            ? LOW_WATERMARK_BYTES - 1
            : randomLongBetween(HIGH_WATERMARK_BYTES, LOW_WATERMARK_BYTES - 1));

        setTotalSpace(dataNodeName, enoughSpace);

        // default 30 minute window includes everything.
        GetAutoscalingCapacityAction.Response response = capacity();
        assertThat(response.results().keySet(), Matchers.equalTo(Set.of(policyName)));
        assertThat(response.results().get(policyName).currentCapacity().total().storage().getBytes(), Matchers.equalTo(enoughSpace));
        // ideally, we would count replicas too, but we leave this for follow-up work
        assertThat(
            response.getResults().get(policyName).toString(),
            response.results().get(policyName).requiredCapacity().total().storage().getBytes(),
            Matchers.greaterThanOrEqualTo(enoughSpace + used)
        );
        assertThat(
            response.results().get(policyName).requiredCapacity().node().storage().getBytes(),
            Matchers.equalTo(maxShardSize + ReactiveStorageDeciderService.NODE_DISK_OVERHEAD + LOW_WATERMARK_BYTES)
        );

        // with 0 window, we expect just current.
        putAutoscalingPolicy(
            policyName,
            Settings.builder().put(ProactiveStorageDeciderService.FORECAST_WINDOW.getKey(), TimeValue.ZERO).build()
        );
        response = capacity();
        assertThat(response.results().keySet(), Matchers.equalTo(Set.of(policyName)));
        assertThat(response.results().get(policyName).currentCapacity().total().storage().getBytes(), Matchers.equalTo(enoughSpace));
        assertThat(response.results().get(policyName).requiredCapacity().total().storage().getBytes(), Matchers.equalTo(enoughSpace));
        assertThat(
            response.results().get(policyName).requiredCapacity().node().storage().getBytes(),
            Matchers.equalTo(maxShardSize + ReactiveStorageDeciderService.NODE_DISK_OVERHEAD + LOW_WATERMARK_BYTES)
        );
    }

    private void putAutoscalingPolicy(String policyName, Settings settings) {
        final PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            policyName,
            new TreeSet<>(Set.of("data")),
            new TreeMap<>(Map.of("proactive_storage", settings))
        );
        assertAcked(client().execute(PutAutoscalingPolicyAction.INSTANCE, request).actionGet());
    }

    private static void createDataStreamAndTemplate(String dataStreamName) throws IOException {
        client().execute(
            PutComposableIndexTemplateAction.INSTANCE,
            new PutComposableIndexTemplateAction.Request(dataStreamName + "_template").indexTemplate(
                new ComposableIndexTemplate(
                    Collections.singletonList(dataStreamName),
                    new Template(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build(), null, null),
                    null,
                    null,
                    null,
                    null,
                    new ComposableIndexTemplate.DataStreamTemplate(),
                    null
                )
            )
        ).actionGet();
        client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(dataStreamName)).actionGet();
    }
}
