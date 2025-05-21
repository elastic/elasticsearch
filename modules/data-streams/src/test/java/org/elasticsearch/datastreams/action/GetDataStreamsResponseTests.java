/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.action;

import org.elasticsearch.action.datastreams.GetDataStreamAction.Response;
import org.elasticsearch.action.datastreams.GetDataStreamAction.Response.ManagedBy;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.DataStream.getDefaultBackingIndexName;
import static org.elasticsearch.cluster.metadata.DataStream.getDefaultFailureStoreName;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetDataStreamsResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::read;
    }

    @Override
    protected Response createTestInstance() {
        int numDataStreams = randomIntBetween(0, 8);
        List<Response.DataStreamInfo> dataStreams = new ArrayList<>();
        for (int i = 0; i < numDataStreams; i++) {
            dataStreams.add(generateRandomDataStreamInfo());
        }
        return new Response(dataStreams);
    }

    @Override
    protected Response mutateInstance(Response instance) {
        if (instance.getDataStreams().isEmpty()) {
            return new Response(List.of(generateRandomDataStreamInfo()));
        }
        return new Response(instance.getDataStreams().stream().map(this::mutateInstance).toList());
    }

    @SuppressWarnings("unchecked")
    public void testResponseIlmAndDataStreamLifecycleRepresentation() throws Exception {
        // we'll test a data stream with 3 backing indices and a failure store - two backing indices managed by ILM (having the ILM policy
        // configured for them) and the remainder without any ILM policy configured
        String dataStreamName = "logs";

        Index firstGenerationIndex = new Index(getDefaultBackingIndexName(dataStreamName, 1), UUIDs.base64UUID());
        Index secondGenerationIndex = new Index(getDefaultBackingIndexName(dataStreamName, 2), UUIDs.base64UUID());
        Index writeIndex = new Index(getDefaultBackingIndexName(dataStreamName, 3), UUIDs.base64UUID());
        Index failureStoreIndex = new Index(getDefaultFailureStoreName(dataStreamName, 1, System.currentTimeMillis()), UUIDs.base64UUID());
        List<Index> indices = List.of(firstGenerationIndex, secondGenerationIndex, writeIndex);
        List<Index> failureStores = List.of(failureStoreIndex);
        {
            // data stream has an enabled lifecycle
            DataStream logs = DataStream.builder("logs", indices)
                .setGeneration(3)
                .setAllowCustomRouting(true)
                .setIndexMode(IndexMode.STANDARD)
                .setLifecycle(DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE)
                .setDataStreamOptions(DataStreamOptions.FAILURE_STORE_ENABLED)
                .setFailureIndices(DataStream.DataStreamIndices.failureIndicesBuilder(failureStores).build())
                .build();

            String ilmPolicyName = "rollover-30days";
            Map<Index, Response.IndexProperties> indexSettingsValues = Map.of(
                firstGenerationIndex,
                new Response.IndexProperties(true, ilmPolicyName, ManagedBy.ILM, null),
                secondGenerationIndex,
                new Response.IndexProperties(false, ilmPolicyName, ManagedBy.LIFECYCLE, null),
                writeIndex,
                new Response.IndexProperties(false, null, ManagedBy.LIFECYCLE, null),
                failureStoreIndex,
                new Response.IndexProperties(false, null, ManagedBy.LIFECYCLE, null)
            );

            Response.DataStreamInfo dataStreamInfo = new Response.DataStreamInfo(
                logs,
                true,
                ClusterHealthStatus.GREEN,
                "index-template",
                null,
                null,
                indexSettingsValues,
                false,
                null,
                null
            );
            Response response = new Response(List.of(dataStreamInfo));
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
            response.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);

            BytesReference bytes = BytesReference.bytes(contentBuilder);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, bytes)) {
                Map<String, Object> map = parser.map();
                List<Object> dataStreams = (List<Object>) map.get(Response.DATA_STREAMS_FIELD.getPreferredName());
                assertThat(dataStreams.size(), is(1));
                Map<String, Object> dataStreamMap = (Map<String, Object>) dataStreams.get(0);
                assertThat(dataStreamMap.get(DataStream.NAME_FIELD.getPreferredName()), is(dataStreamName));

                assertThat(dataStreamMap.get(Response.DataStreamInfo.PREFER_ILM.getPreferredName()), is(false));
                assertThat(dataStreamMap.get(Response.DataStreamInfo.ILM_POLICY_FIELD.getPreferredName()), is(nullValue()));
                assertThat(dataStreamMap.get(Response.DataStreamInfo.LIFECYCLE_FIELD.getPreferredName()), is(Map.of("enabled", true)));
                assertThat(
                    dataStreamMap.get(Response.DataStreamInfo.NEXT_GENERATION_INDEX_MANAGED_BY.getPreferredName()),
                    is(ManagedBy.LIFECYCLE.displayValue)
                );

                List<Object> indicesRepresentation = (List<Object>) dataStreamMap.get(DataStream.INDICES_FIELD.getPreferredName());
                Map<String, Object> firstGenIndexRepresentation = (Map<String, Object>) indicesRepresentation.get(0);
                assertThat(firstGenIndexRepresentation.get("index_name"), is(firstGenerationIndex.getName()));
                assertThat(firstGenIndexRepresentation.get(Response.DataStreamInfo.PREFER_ILM.getPreferredName()), is(true));
                assertThat(firstGenIndexRepresentation.get(Response.DataStreamInfo.ILM_POLICY_FIELD.getPreferredName()), is(ilmPolicyName));
                assertThat(
                    firstGenIndexRepresentation.get(Response.DataStreamInfo.MANAGED_BY.getPreferredName()),
                    is(ManagedBy.ILM.displayValue)
                );

                Map<String, Object> secondGenIndexRepresentation = (Map<String, Object>) indicesRepresentation.get(1);
                assertThat(secondGenIndexRepresentation.get("index_name"), is(secondGenerationIndex.getName()));
                assertThat(secondGenIndexRepresentation.get(Response.DataStreamInfo.PREFER_ILM.getPreferredName()), is(false));
                assertThat(
                    secondGenIndexRepresentation.get(Response.DataStreamInfo.ILM_POLICY_FIELD.getPreferredName()),
                    is(ilmPolicyName)
                );
                assertThat(
                    secondGenIndexRepresentation.get(Response.DataStreamInfo.MANAGED_BY.getPreferredName()),
                    is(ManagedBy.LIFECYCLE.displayValue)
                );

                // the write index is managed by data stream lifecycle
                Map<String, Object> writeIndexRepresentation = (Map<String, Object>) indicesRepresentation.get(2);
                assertThat(writeIndexRepresentation.get("index_name"), is(writeIndex.getName()));
                assertThat(writeIndexRepresentation.get(Response.DataStreamInfo.PREFER_ILM.getPreferredName()), is(false));
                assertThat(writeIndexRepresentation.get(Response.DataStreamInfo.ILM_POLICY_FIELD.getPreferredName()), is(nullValue()));
                assertThat(
                    writeIndexRepresentation.get(Response.DataStreamInfo.MANAGED_BY.getPreferredName()),
                    is(ManagedBy.LIFECYCLE.displayValue)
                );

                var failureStore = (Map<String, Object>) dataStreamMap.get(DataStream.FAILURE_STORE_FIELD.getPreferredName());
                List<Object> failureIndices = (List<Object>) failureStore.get(DataStream.INDICES_FIELD.getPreferredName());
                Map<String, Object> failureIndexRepresentation = (Map<String, Object>) failureIndices.get(0);
                assertThat(failureIndexRepresentation.get("index_name"), is(failureStoreIndex.getName()));
                assertThat(failureIndexRepresentation.get(Response.DataStreamInfo.PREFER_ILM.getPreferredName()), nullValue());
                assertThat(failureIndexRepresentation.get(Response.DataStreamInfo.ILM_POLICY_FIELD.getPreferredName()), is(nullValue()));
                assertThat(
                    failureIndexRepresentation.get(Response.DataStreamInfo.MANAGED_BY.getPreferredName()),
                    is(ManagedBy.LIFECYCLE.displayValue)
                );
            }
        }

        {
            // data stream has a lifecycle that's not enabled
            DataStream logs = DataStream.builder("logs", indices)
                .setGeneration(3)
                .setAllowCustomRouting(true)
                .setIndexMode(IndexMode.STANDARD)
                .setLifecycle(DataStreamLifecycle.createDataLifecycle(false, null, null))
                .setDataStreamOptions(DataStreamOptions.FAILURE_STORE_ENABLED)
                .setFailureIndices(DataStream.DataStreamIndices.failureIndicesBuilder(failureStores).build())
                .build();

            String ilmPolicyName = "rollover-30days";
            Map<Index, Response.IndexProperties> indexSettingsValues = Map.of(
                firstGenerationIndex,
                new Response.IndexProperties(true, ilmPolicyName, ManagedBy.ILM, null),
                secondGenerationIndex,
                new Response.IndexProperties(true, ilmPolicyName, ManagedBy.ILM, null),
                writeIndex,
                new Response.IndexProperties(false, null, ManagedBy.UNMANAGED, null),
                failureStoreIndex,
                new Response.IndexProperties(false, null, ManagedBy.UNMANAGED, null)
            );

            Response.DataStreamInfo dataStreamInfo = new Response.DataStreamInfo(
                logs,
                true,
                ClusterHealthStatus.GREEN,
                "index-template",
                null,
                null,
                indexSettingsValues,
                false,
                null,
                null
            );
            Response response = new Response(List.of(dataStreamInfo));
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
            response.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);

            BytesReference bytes = BytesReference.bytes(contentBuilder);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, bytes)) {
                Map<String, Object> map = parser.map();
                List<Object> dataStreams = (List<Object>) map.get(Response.DATA_STREAMS_FIELD.getPreferredName());
                assertThat(dataStreams.size(), is(1));
                Map<String, Object> dataStreamMap = (Map<String, Object>) dataStreams.get(0);
                assertThat(dataStreamMap.get(DataStream.NAME_FIELD.getPreferredName()), is(dataStreamName));
                // note that the prefer_ilm value is displayed at the top level even if the template backing the data stream doesn't have a
                // policy specified anymore
                assertThat(dataStreamMap.get(Response.DataStreamInfo.PREFER_ILM.getPreferredName()), is(false));
                assertThat(dataStreamMap.get(Response.DataStreamInfo.ILM_POLICY_FIELD.getPreferredName()), is(nullValue()));
                assertThat(dataStreamMap.get(Response.DataStreamInfo.LIFECYCLE_FIELD.getPreferredName()), is(Map.of("enabled", false)));
                assertThat(
                    dataStreamMap.get(Response.DataStreamInfo.NEXT_GENERATION_INDEX_MANAGED_BY.getPreferredName()),
                    is(ManagedBy.UNMANAGED.displayValue)
                );

                List<Object> indicesRepresentation = (List<Object>) dataStreamMap.get(DataStream.INDICES_FIELD.getPreferredName());
                Map<String, Object> firstGenIndexRepresentation = (Map<String, Object>) indicesRepresentation.get(0);
                assertThat(firstGenIndexRepresentation.get("index_name"), is(firstGenerationIndex.getName()));
                assertThat(firstGenIndexRepresentation.get(Response.DataStreamInfo.PREFER_ILM.getPreferredName()), is(true));
                assertThat(firstGenIndexRepresentation.get(Response.DataStreamInfo.ILM_POLICY_FIELD.getPreferredName()), is(ilmPolicyName));
                assertThat(
                    firstGenIndexRepresentation.get(Response.DataStreamInfo.MANAGED_BY.getPreferredName()),
                    is(ManagedBy.ILM.displayValue)
                );

                // the write index is managed by data stream lifecycle
                Map<String, Object> writeIndexRepresentation = (Map<String, Object>) indicesRepresentation.get(2);
                assertThat(writeIndexRepresentation.get("index_name"), is(writeIndex.getName()));
                assertThat(writeIndexRepresentation.get(Response.DataStreamInfo.PREFER_ILM.getPreferredName()), is(false));
                assertThat(writeIndexRepresentation.get(Response.DataStreamInfo.ILM_POLICY_FIELD.getPreferredName()), is(nullValue()));
                assertThat(
                    writeIndexRepresentation.get(Response.DataStreamInfo.MANAGED_BY.getPreferredName()),
                    is(ManagedBy.UNMANAGED.displayValue)
                );

                var failureStore = (Map<String, Object>) dataStreamMap.get(DataStream.FAILURE_STORE_FIELD.getPreferredName());
                List<Object> failureStoresRepresentation = (List<Object>) failureStore.get(DataStream.INDICES_FIELD.getPreferredName());
                Map<String, Object> failureStoreRepresentation = (Map<String, Object>) failureStoresRepresentation.get(0);
                assertThat(failureStoreRepresentation.get("index_name"), is(failureStoreIndex.getName()));
                assertThat(failureStoreRepresentation.get(Response.DataStreamInfo.PREFER_ILM.getPreferredName()), nullValue());
                assertThat(failureStoreRepresentation.get(Response.DataStreamInfo.ILM_POLICY_FIELD.getPreferredName()), is(nullValue()));
                assertThat(
                    failureStoreRepresentation.get(Response.DataStreamInfo.MANAGED_BY.getPreferredName()),
                    is(ManagedBy.UNMANAGED.displayValue)
                );
            }
        }

        {
            // one failure index that have ILM policy
            DataStream logs = DataStream.builder("logs", indices)
                .setGeneration(3)
                .setAllowCustomRouting(true)
                .setIndexMode(IndexMode.STANDARD)
                .setLifecycle(DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE)
                .setDataStreamOptions(DataStreamOptions.FAILURE_STORE_ENABLED)
                .setFailureIndices(DataStream.DataStreamIndices.failureIndicesBuilder(failureStores).build())
                .build();

            String ilmPolicyName = "rollover-30days";
            Map<Index, Response.IndexProperties> indexSettingsValues = Map.of(
                firstGenerationIndex,
                new Response.IndexProperties(true, ilmPolicyName, ManagedBy.ILM, null),
                secondGenerationIndex,
                new Response.IndexProperties(false, ilmPolicyName, ManagedBy.LIFECYCLE, null),
                writeIndex,
                new Response.IndexProperties(true, null, ManagedBy.LIFECYCLE, null),
                failureStoreIndex,
                new Response.IndexProperties(randomBoolean(), ilmPolicyName, ManagedBy.LIFECYCLE, null)
            );

            Response.DataStreamInfo dataStreamInfo = new Response.DataStreamInfo(
                logs,
                true,
                ClusterHealthStatus.GREEN,
                "index-template",
                null,
                null,
                indexSettingsValues,
                false,
                null,
                null
            );
            Response response = new Response(List.of(dataStreamInfo));
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
            response.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);

            BytesReference bytes = BytesReference.bytes(contentBuilder);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, bytes)) {
                Map<String, Object> map = parser.map();
                List<Object> dataStreams = (List<Object>) map.get(Response.DATA_STREAMS_FIELD.getPreferredName());
                assertThat(dataStreams.size(), is(1));
                Map<String, Object> dataStreamMap = (Map<String, Object>) dataStreams.get(0);
                assertThat(dataStreamMap.get(DataStream.NAME_FIELD.getPreferredName()), is(dataStreamName));

                var failureStore = (Map<String, Object>) dataStreamMap.get(DataStream.FAILURE_STORE_FIELD.getPreferredName());
                List<Object> failureIndices = (List<Object>) failureStore.get(DataStream.INDICES_FIELD.getPreferredName());
                Map<String, Object> failureIndexRepresentation = (Map<String, Object>) failureIndices.get(0);
                assertThat(failureIndexRepresentation.get("index_name"), is(failureStoreIndex.getName()));
                assertThat(failureIndexRepresentation.get(Response.DataStreamInfo.PREFER_ILM.getPreferredName()), notNullValue());
                assertThat(failureIndexRepresentation.get(Response.DataStreamInfo.ILM_POLICY_FIELD.getPreferredName()), is(ilmPolicyName));
                assertThat(
                    failureIndexRepresentation.get(Response.DataStreamInfo.MANAGED_BY.getPreferredName()),
                    is(ManagedBy.LIFECYCLE.displayValue)
                );
            }
        }
    }

    public void testManagedByDisplayValuesDontAccidentalyChange() {
        // UI might derive logic based on the display values so any changes should be coordinated with the UI team
        assertThat(ManagedBy.ILM.displayValue, is("Index Lifecycle Management"));
        assertThat(ManagedBy.LIFECYCLE.displayValue, is("Data stream lifecycle"));
        assertThat(ManagedBy.UNMANAGED.displayValue, is("Unmanaged"));
    }

    private Response.DataStreamInfo mutateInstance(Response.DataStreamInfo instance) {
        var dataStream = instance.getDataStream();
        var failureStoreEffectivelyEnabled = instance.isFailureStoreEffectivelyEnabled();
        var status = instance.getDataStreamStatus();
        var indexTemplate = instance.getIndexTemplate();
        var ilmPolicyName = instance.getIlmPolicy();
        var timeSeries = instance.getTimeSeries();
        var indexSettings = instance.getIndexSettingsValues();
        var templatePreferIlm = instance.templatePreferIlmValue();
        var maximumTimestamp = instance.getMaximumTimestamp();
        switch (randomIntBetween(0, 8)) {
            case 0 -> dataStream = randomValueOtherThan(dataStream, DataStreamTestHelper::randomInstance);
            case 1 -> status = randomValueOtherThan(status, () -> randomFrom(ClusterHealthStatus.values()));
            case 2 -> indexTemplate = randomBoolean() && indexTemplate != null ? null : randomAlphaOfLengthBetween(2, 10);
            case 3 -> ilmPolicyName = randomBoolean() && ilmPolicyName != null ? null : randomAlphaOfLengthBetween(2, 10);
            case 4 -> timeSeries = randomBoolean() && timeSeries != null
                ? null
                : randomValueOtherThan(timeSeries, () -> new Response.TimeSeries(generateRandomTimeSeries()));
            case 5 -> indexSettings = randomValueOtherThan(
                indexSettings,
                () -> randomBoolean()
                    ? Map.of()
                    : Map.of(
                        new Index(randomAlphaOfLengthBetween(50, 100), UUIDs.base64UUID()),
                        new Response.IndexProperties(
                            randomBoolean(),
                            randomAlphaOfLengthBetween(50, 100),
                            randomBoolean() ? ManagedBy.ILM : ManagedBy.LIFECYCLE,
                            null
                        )
                    )
            );
            case 6 -> templatePreferIlm = templatePreferIlm ? false : true;
            case 7 -> maximumTimestamp = (maximumTimestamp == null)
                ? randomNonNegativeLong()
                : (usually() ? randomValueOtherThan(maximumTimestamp, ESTestCase::randomNonNegativeLong) : null);
            case 8 -> failureStoreEffectivelyEnabled = failureStoreEffectivelyEnabled ? false : true;
        }
        return new Response.DataStreamInfo(
            dataStream,
            failureStoreEffectivelyEnabled,
            status,
            indexTemplate,
            ilmPolicyName,
            timeSeries,
            indexSettings,
            templatePreferIlm,
            maximumTimestamp,
            null
        );
    }

    private List<Tuple<Instant, Instant>> generateRandomTimeSeries() {
        List<Tuple<Instant, Instant>> timeSeries = new ArrayList<>();
        int numTimeSeries = randomIntBetween(0, 3);
        for (int j = 0; j < numTimeSeries; j++) {
            timeSeries.add(new Tuple<>(Instant.now(), Instant.now()));
        }
        return timeSeries;
    }

    private Map<Index, Response.IndexProperties> generateRandomIndexSettingsValues() {
        Map<Index, Response.IndexProperties> values = new HashMap<>();
        for (int i = 0; i < randomIntBetween(0, 3); i++) {
            values.put(
                new Index(randomAlphaOfLengthBetween(50, 100), UUIDs.base64UUID()),
                new Response.IndexProperties(
                    randomBoolean(),
                    randomAlphaOfLengthBetween(50, 100),
                    randomBoolean() ? ManagedBy.ILM : ManagedBy.LIFECYCLE,
                    randomBoolean() ? randomFrom(IndexMode.values()).getName() : null
                )
            );
        }
        return values;
    }

    private Response.DataStreamInfo generateRandomDataStreamInfo() {
        List<Tuple<Instant, Instant>> timeSeries = randomBoolean() ? generateRandomTimeSeries() : null;
        return new Response.DataStreamInfo(
            DataStreamTestHelper.randomInstance(),
            randomBoolean(),
            ClusterHealthStatus.GREEN,
            randomAlphaOfLengthBetween(2, 10),
            randomAlphaOfLengthBetween(2, 10),
            timeSeries != null ? new Response.TimeSeries(timeSeries) : null,
            generateRandomIndexSettingsValues(),
            randomBoolean(),
            usually() ? randomNonNegativeLong() : null,
            usually() ? randomFrom(IndexMode.values()).getName() : null
        );
    }
}
