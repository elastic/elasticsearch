/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetection;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class DestinationIndexTests extends ESTestCase {

    private static final String ANALYTICS_ID = "some-analytics-id";
    private static final String[] SOURCE_INDEX = new String[] {"source-index"};
    private static final String DEST_INDEX = "dest-index";
    private static final String NUMERICAL_FIELD = "numerical-field";
    private static final String OUTER_FIELD = "outer-field";
    private static final String INNER_FIELD = "inner-field";
    private static final String ALIAS_TO_NUMERICAL_FIELD = "alias-to-numerical-field";
    private static final String ALIAS_TO_NESTED_FIELD = "alias-to-nested-field";
    private static final int CURRENT_TIME_MILLIS = 123456789;
    private static final String CREATED_BY = "data-frame-analytics";

    private ThreadPool threadPool = mock(ThreadPool.class);
    private Client client = mock(Client.class);
    private Clock clock = Clock.fixed(Instant.ofEpochMilli(123456789L), ZoneId.systemDefault());

    @Before
    public void setUpMocks() {
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
    }

    private Map<String, Object> testCreateDestinationIndex(DataFrameAnalysis analysis) throws IOException {
        DataFrameAnalyticsConfig config = createConfig(analysis);

        ArgumentCaptor<CreateIndexRequest> createIndexRequestCaptor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        doAnswer(callListenerOnResponse(null))
            .when(client).execute(eq(CreateIndexAction.INSTANCE), createIndexRequestCaptor.capture(), any());

        Settings index1Settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        Settings index2Settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 5)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();

        ArgumentCaptor<GetSettingsRequest> getSettingsRequestCaptor = ArgumentCaptor.forClass(GetSettingsRequest.class);
        ArgumentCaptor<GetMappingsRequest> getMappingsRequestCaptor = ArgumentCaptor.forClass(GetMappingsRequest.class);

        ImmutableOpenMap.Builder<String, Settings> indexToSettings = ImmutableOpenMap.builder();
        indexToSettings.put("index_1", index1Settings);
        indexToSettings.put("index_2", index2Settings);

        GetSettingsResponse getSettingsResponse = new GetSettingsResponse(indexToSettings.build(), ImmutableOpenMap.of());

        doAnswer(callListenerOnResponse(getSettingsResponse))
            .when(client).execute(eq(GetSettingsAction.INSTANCE), getSettingsRequestCaptor.capture(), any());

        Map<String, Object> indexMappings =
            Map.of(
                "properties",
                Map.of(
                    "field_1", "field_1_mappings",
                    "field_2", "field_2_mappings",
                    NUMERICAL_FIELD, Map.of("type", "integer"),
                    OUTER_FIELD, Map.of("properties", Map.of(INNER_FIELD, Map.of("type", "integer"))),
                    ALIAS_TO_NUMERICAL_FIELD, Map.of("type", "alias", "path", NUMERICAL_FIELD),
                    ALIAS_TO_NESTED_FIELD, Map.of("type", "alias", "path", "outer-field.inner-field")));
        MappingMetaData index1MappingMetaData = new MappingMetaData("_doc", indexMappings);
        MappingMetaData index2MappingMetaData = new MappingMetaData("_doc", indexMappings);

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder();
        mappings.put("index_1", index1MappingMetaData);
        mappings.put("index_2", index2MappingMetaData);

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        doAnswer(callListenerOnResponse(getMappingsResponse))
            .when(client).execute(eq(GetMappingsAction.INSTANCE), getMappingsRequestCaptor.capture(), any());

        DestinationIndex.createDestinationIndex(
            client,
            clock,
            config,
            ActionListener.wrap(
                response -> {},
                e -> fail(e.getMessage())
            )
        );

        GetSettingsRequest capturedGetSettingsRequest = getSettingsRequestCaptor.getValue();
        assertThat(capturedGetSettingsRequest.indices(), equalTo(SOURCE_INDEX));
        assertThat(capturedGetSettingsRequest.indicesOptions(), equalTo(IndicesOptions.lenientExpandOpen()));
        assertThat(Arrays.asList(capturedGetSettingsRequest.names()), contains("index.number_of_shards", "index.number_of_replicas"));

        assertThat(getMappingsRequestCaptor.getValue().indices(), equalTo(SOURCE_INDEX));

        CreateIndexRequest createIndexRequest = createIndexRequestCaptor.getValue();

        assertThat(createIndexRequest.settings().keySet(),
            containsInAnyOrder("index.number_of_shards", "index.number_of_replicas", "index.sort.field", "index.sort.order"));
        assertThat(createIndexRequest.settings().getAsInt("index.number_of_shards", -1), equalTo(5));
        assertThat(createIndexRequest.settings().getAsInt("index.number_of_replicas", -1), equalTo(1));
        assertThat(createIndexRequest.settings().get("index.sort.field"), equalTo("ml__id_copy"));
        assertThat(createIndexRequest.settings().get("index.sort.order"), equalTo("asc"));

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, createIndexRequest.mappings())) {
            Map<String, Object> map = parser.map();
            assertThat(extractValue("_doc.properties.ml__id_copy.type", map), equalTo("keyword"));
            assertThat(extractValue("_doc.properties.field_1", map), equalTo("field_1_mappings"));
            assertThat(extractValue("_doc.properties.field_2", map), equalTo("field_2_mappings"));
            assertThat(extractValue("_doc.properties.numerical-field.type", map), equalTo("integer"));
            assertThat(extractValue("_doc.properties.outer-field.properties.inner-field.type", map), equalTo("integer"));
            assertThat(extractValue("_doc.properties.alias-to-numerical-field.type", map), equalTo("alias"));
            assertThat(extractValue("_doc.properties.alias-to-nested-field.type", map), equalTo("alias"));
            assertThat(extractValue("_doc._meta.analytics", map), equalTo(ANALYTICS_ID));
            assertThat(extractValue("_doc._meta.creation_date_in_millis", map), equalTo(CURRENT_TIME_MILLIS));
            assertThat(extractValue("_doc._meta.created_by", map), equalTo(CREATED_BY));
            return map;
        }
    }

    public void testCreateDestinationIndex_OutlierDetection() throws IOException {
        testCreateDestinationIndex(new OutlierDetection.Builder().build());
    }

    public void testCreateDestinationIndex_Regression() throws IOException {
        Map<String, Object> map = testCreateDestinationIndex(new Regression(NUMERICAL_FIELD));
        assertThat(extractValue("_doc.properties.ml.numerical-field_prediction.type", map), equalTo("double"));
    }

    public void testCreateDestinationIndex_Classification() throws IOException {
        Map<String, Object> map = testCreateDestinationIndex(new Classification(NUMERICAL_FIELD));
        assertThat(extractValue("_doc.properties.ml.numerical-field_prediction.type", map), equalTo("integer"));
        assertThat(extractValue("_doc.properties.ml.top_classes.class_name.type", map), equalTo("integer"));
    }

    public void testCreateDestinationIndex_Classification_DependentVariableIsNested() throws IOException {
        Map<String, Object> map = testCreateDestinationIndex(new Classification(OUTER_FIELD + "." + INNER_FIELD));
        assertThat(extractValue("_doc.properties.ml.outer-field.inner-field_prediction.type", map), equalTo("integer"));
        assertThat(extractValue("_doc.properties.ml.top_classes.class_name.type", map), equalTo("integer"));
    }

    public void testCreateDestinationIndex_Classification_DependentVariableIsAlias() throws IOException {
        Map<String, Object> map = testCreateDestinationIndex(new Classification(ALIAS_TO_NUMERICAL_FIELD));
        assertThat(extractValue("_doc.properties.ml.alias-to-numerical-field_prediction.type", map), equalTo("integer"));
        assertThat(extractValue("_doc.properties.ml.top_classes.class_name.type", map), equalTo("integer"));
    }

    public void testCreateDestinationIndex_Classification_DependentVariableIsAliasToNested() throws IOException {
        Map<String, Object> map = testCreateDestinationIndex(new Classification(ALIAS_TO_NESTED_FIELD));
        assertThat(extractValue("_doc.properties.ml.alias-to-nested-field_prediction.type", map), equalTo("integer"));
        assertThat(extractValue("_doc.properties.ml.top_classes.class_name.type", map), equalTo("integer"));
    }

    public void testCreateDestinationIndex_ResultsFieldsExistsInSourceIndex() {
        DataFrameAnalyticsConfig config = createConfig(new OutlierDetection.Builder().build());

        GetSettingsResponse getSettingsResponse = new GetSettingsResponse(ImmutableOpenMap.of(), ImmutableOpenMap.of());

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder();
        mappings.put("", new MappingMetaData("_doc", Map.of("properties", Map.of("ml", "some-mapping"))));
        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings.build());

        doAnswer(callListenerOnResponse(getSettingsResponse)).when(client).execute(eq(GetSettingsAction.INSTANCE), any(), any());
        doAnswer(callListenerOnResponse(getMappingsResponse)).when(client).execute(eq(GetMappingsAction.INSTANCE), any(), any());

        DestinationIndex.createDestinationIndex(
            client,
            clock,
            config,
            ActionListener.wrap(
                response -> fail("should not succeed"),
                e -> assertThat(
                    e.getMessage(),
                    equalTo("A field that matches the dest.results_field [ml] already exists; please set a different results_field"))
            )
        );
    }

    private Map<String, Object> testUpdateMappingsToDestIndex(DataFrameAnalysis analysis) throws IOException {
        DataFrameAnalyticsConfig config = createConfig(analysis);

        Map<String, Object> properties = Map.of(
            NUMERICAL_FIELD, Map.of("type", "integer"),
            OUTER_FIELD, Map.of("properties", Map.of(INNER_FIELD, Map.of("type", "integer"))),
            ALIAS_TO_NUMERICAL_FIELD, Map.of("type", "alias", "path", NUMERICAL_FIELD),
            ALIAS_TO_NESTED_FIELD, Map.of("type", "alias", "path", OUTER_FIELD + "." + INNER_FIELD)
        );
        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder();
        mappings.put("", new MappingMetaData("_doc", Map.of("properties", properties)));
        GetIndexResponse getIndexResponse =
            new GetIndexResponse(
                new String[] { DEST_INDEX }, mappings.build(), ImmutableOpenMap.of(), ImmutableOpenMap.of(), ImmutableOpenMap.of());

        ArgumentCaptor<PutMappingRequest> putMappingRequestCaptor = ArgumentCaptor.forClass(PutMappingRequest.class);

        doAnswer(callListenerOnResponse(new AcknowledgedResponse(true)))
            .when(client).execute(eq(PutMappingAction.INSTANCE), putMappingRequestCaptor.capture(), any());

        DestinationIndex.updateMappingsToDestIndex(
            client,
            config,
            getIndexResponse,
            ActionListener.wrap(
                response -> assertThat(response.isAcknowledged(), is(true)),
                e -> fail(e.getMessage())
            )
        );

        verify(client, atLeastOnce()).threadPool();
        verify(client).execute(eq(PutMappingAction.INSTANCE), any(), any());
        verifyNoMoreInteractions(client);

        PutMappingRequest putMappingRequest = putMappingRequestCaptor.getValue();
        assertThat(putMappingRequest.indices(), arrayContaining(DEST_INDEX));
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, putMappingRequest.source())) {
            Map<String, Object> map = parser.map();
            assertThat(extractValue("properties.ml__id_copy.type", map), equalTo("keyword"));
            return map;
        }
    }

    public void testUpdateMappingsToDestIndex_OutlierDetection() throws IOException {
        testUpdateMappingsToDestIndex(new OutlierDetection.Builder().build());
    }

    public void testUpdateMappingsToDestIndex_Regression() throws IOException {
        Map<String, Object> map = testUpdateMappingsToDestIndex(new Regression(NUMERICAL_FIELD));
        assertThat(extractValue("properties.ml.numerical-field_prediction.type", map), equalTo("double"));
    }

    public void testUpdateMappingsToDestIndex_Classification() throws IOException {
        Map<String, Object> map = testUpdateMappingsToDestIndex(new Classification(NUMERICAL_FIELD));
        assertThat(extractValue("properties.ml.numerical-field_prediction.type", map), equalTo("integer"));
        assertThat(extractValue("properties.ml.top_classes.class_name.type", map), equalTo("integer"));
    }

    public void testUpdateMappingsToDestIndex_Classification_DependentVariableIsNested() throws IOException {
        Map<String, Object> map = testUpdateMappingsToDestIndex(new Classification(OUTER_FIELD + "." + INNER_FIELD));
        assertThat(extractValue("properties.ml.outer-field.inner-field_prediction.type", map), equalTo("integer"));
        assertThat(extractValue("properties.ml.top_classes.class_name.type", map), equalTo("integer"));
    }

    public void testUpdateMappingsToDestIndex_Classification_DependentVariableIsAlias() throws IOException {
        Map<String, Object> map = testUpdateMappingsToDestIndex(new Classification(ALIAS_TO_NUMERICAL_FIELD));
        assertThat(extractValue("properties.ml.alias-to-numerical-field_prediction.type", map), equalTo("integer"));
        assertThat(extractValue("properties.ml.top_classes.class_name.type", map), equalTo("integer"));
    }

    public void testUpdateMappingsToDestIndex_Classification_DependentVariableIsAliasToNested() throws IOException {
        Map<String, Object> map = testUpdateMappingsToDestIndex(new Classification(ALIAS_TO_NESTED_FIELD));
        assertThat(extractValue("properties.ml.alias-to-nested-field_prediction.type", map), equalTo("integer"));
        assertThat(extractValue("properties.ml.top_classes.class_name.type", map), equalTo("integer"));
    }

    public void testUpdateMappingsToDestIndex_ResultsFieldsExistsInSourceIndex() {
        DataFrameAnalyticsConfig config = createConfig(new OutlierDetection.Builder().build());

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder();
        mappings.put("", new MappingMetaData("_doc", Map.of("properties", Map.of("ml", "some-mapping"))));
        GetIndexResponse getIndexResponse =
            new GetIndexResponse(
                new String[] { DEST_INDEX }, mappings.build(), ImmutableOpenMap.of(), ImmutableOpenMap.of(), ImmutableOpenMap.of());

        ElasticsearchStatusException e =
            expectThrows(
                ElasticsearchStatusException.class,
                () -> DestinationIndex.updateMappingsToDestIndex(
                    client, config, getIndexResponse, ActionListener.wrap(Assert::fail)));
        assertThat(
            e.getMessage(),
            equalTo("A field that matches the dest.results_field [ml] already exists; please set a different results_field"));

        verifyZeroInteractions(client);
    }

    private static <Response> Answer<Response> callListenerOnResponse(Response response) {
        return invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        };
    }

    private static DataFrameAnalyticsConfig createConfig(DataFrameAnalysis analysis) {
        return new DataFrameAnalyticsConfig.Builder()
            .setId(ANALYTICS_ID)
            .setSource(new DataFrameAnalyticsSource(SOURCE_INDEX, null, null))
            .setDest(new DataFrameAnalyticsDest(DEST_INDEX, null))
            .setAnalysis(analysis)
            .build();
    }
}
