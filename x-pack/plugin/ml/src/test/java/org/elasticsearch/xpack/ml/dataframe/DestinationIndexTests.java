/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesBuilder;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetection;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.action.support.ActionTestUtils.assertNoSuccessListener;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.xpack.ml.DefaultMachineLearningExtension.ANALYTICS_DEST_INDEX_ALLOWED_SETTINGS;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class DestinationIndexTests extends ESTestCase {

    private static final String ANALYTICS_ID = "some-analytics-id";
    private static final String[] SOURCE_INDEX = new String[] { "source-index" };
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

    private enum ExpectedError {
        NONE,
        INDEX_SIMILARITY,
        INDEX_ANALYSIS_FILTER,
        INDEX_ANALYSIS_ANALYZER
    }

    @Before
    public void setUpMocks() {
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
    }

    private Map<String, Object> testCreateDestinationIndex(DataFrameAnalysis analysis, ExpectedError expectedError) throws IOException {
        DataFrameAnalyticsConfig config = createConfig(analysis);

        ArgumentCaptor<CreateIndexRequest> createIndexRequestCaptor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        doAnswer(callListenerOnResponse(null)).when(client)
            .execute(eq(TransportCreateIndexAction.TYPE), createIndexRequestCaptor.capture(), any());

        Map<String, Object> analysisSettings1 = Map.ofEntries(
            Map.entry("index.analysis.filter.bigram_joiner.max_shingle_size", "2"),
            Map.entry("index.analysis.filter.bigram_joiner.output_unigrams", "false"),
            Map.entry("index.analysis.filter.bigram_joiner.token_separator", ""),
            Map.entry("index.analysis.filter.bigram_joiner.type", "shingle"),
            Map.entry("index.analysis.filter.bigram_max_size.max", "16"),
            Map.entry("index.analysis.filter.bigram_max_size.min", "0"),
            Map.entry("index.analysis.filter.bigram_max_size.type", "length"),
            Map.entry("index.analysis.filter.en-stem-filter.name", "light_english"),
            Map.entry(
                "index.analysis.filter.en-stem-filter.type",
                (expectedError == ExpectedError.INDEX_ANALYSIS_FILTER) ? "foobarbaz" : "stemmer"
            ),
            Map.entry("index.analysis.filter.bigram_joiner_unigrams.max_shingle_size", "2"),
            Map.entry("index.analysis.filter.bigram_joiner_unigrams.output_unigrams", "true"),
            Map.entry("index.analysis.filter.bigram_joiner_unigrams.token_separator", ""),
            Map.entry("index.analysis.filter.bigram_joiner_unigrams.type", "shingle"),
            Map.entry("index.analysis.filter.en-stop-words-filter.stopwords", "_english_"),
            Map.entry("index.analysis.filter.en-stop-words-filter.type", "stop"),
            Map.entry("index.analysis.analyzer.i_prefix.filter", List.of("cjk_width", "lowercase", "asciifolding", "front_ngram")),
            Map.entry("index.analysis.analyzer.i_prefix.tokenizer", "standard"),
            Map.entry(
                "index.analysis.analyzer.iq_text_delimiter.filter",
                (expectedError == ExpectedError.INDEX_ANALYSIS_ANALYZER)
                    ? List.of("delimiter", "cjk_width", "lowercase", "en-stop-words-filter", "en-stem-filter")
                    : List.of("delimiter", "cjk_width", "lowercase", "asciifolding", "en-stop-words-filter", "en-stem-filter")
            ),
            Map.entry("index.analysis.analyzer.iq_text_delimiter.tokenizer", "whitespace"),
            Map.entry("index.analysis.analyzer.q_prefix.filter", List.of("cjk_width", "lowercase", "asciifolding")),
            Map.entry("index.analysis.analyzer.q_prefix.tokenizer", "standard"),
            Map.entry(
                "index.analysis.analyzer.iq_text_base.filter",
                List.of("cjk_width", "lowercase", "asciifolding", "en-stop-words-filter")
            ),
            Map.entry("index.analysis.analyzer.iq_text_base.tokenizer", "standard"),
            Map.entry(
                "index.analysis.analyzer.iq_text_stem.filter",
                List.of("cjk_width", "lowercase", "asciifolding", "en-stop-words-filter", "en-stem-filter")
            ),
            Map.entry("index.analysis.analyzer.iq_text_stem.tokenizer", "standard"),
            Map.entry(
                "index.analysis.analyzer.i_text_bigram.filter",
                List.of("cjk_width", "lowercase", "asciifolding", "en-stem-filter", "bigram_joiner", "bigram_max_size")
            ),
            Map.entry("index.analysis.analyzer.i_text_bigram.tokenizer", "standard"),
            Map.entry(
                "index.analysis.analyzer.q_text_bigram.filter",
                List.of("cjk_width", "lowercase", "asciifolding", "en-stem-filter", "bigram_joiner_unigrams", "bigram_max_size")
            ),
            Map.entry("index.analysis.analyzer.q_text_bigram.tokenizer", "standard")
        );

        Settings.Builder index1SettingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.mapping.total_fields.limit", 1000)
            .put("index.mapping.depth.limit", 20)
            .put("index.mapping.nested_fields.limit", 50)
            .put("index.mapping.nested_objects.limit", 10000)
            .put("index.mapping.field_name_length.limit", Long.MAX_VALUE)
            .put("index.mapping.dimension_fields.limit", 16)
            .put("index.similarity.default", "bm25");
        index1SettingsBuilder.loadFromMap(analysisSettings1);
        Settings index1Settings = index1SettingsBuilder.build();

        Map<String, Object> analysisSettings2 = Map.ofEntries(
            Map.entry("index.analysis.filter.front_ngram.max_gram", "12"),
            Map.entry("index.analysis.filter.front_ngram.min_gram", "1"),
            Map.entry("index.analysis.filter.front_ngram.type", "edge_ngram"),
            Map.entry("index.analysis.filter.bigram_joiner.max_shingle_size", "2"),
            Map.entry("index.analysis.filter.bigram_joiner.output_unigrams", "false"),
            Map.entry("index.analysis.filter.bigram_joiner.token_separator", ""),
            Map.entry("index.analysis.filter.bigram_joiner.type", "shingle"),
            Map.entry("index.analysis.filter.bigram_max_size.max", "16"),
            Map.entry("index.analysis.filter.bigram_max_size.min", "0"),
            Map.entry("index.analysis.filter.bigram_max_size.type", "length"),
            Map.entry("index.analysis.filter.en-stem-filter.name", "light_english"),
            Map.entry("index.analysis.filter.en-stem-filter.type", "stemmer"),
            Map.entry("index.analysis.filter.bigram_joiner_unigrams.max_shingle_size", "2"),
            Map.entry("index.analysis.filter.bigram_joiner_unigrams.output_unigrams", "true"),
            Map.entry("index.analysis.filter.bigram_joiner_unigrams.token_separator", ""),
            Map.entry("index.analysis.filter.bigram_joiner_unigrams.type", "shingle"),
            Map.entry("index.analysis.filter.en-stop-words-filter.stopwords", "_english_"),
            Map.entry("index.analysis.filter.en-stop-words-filter.type", "stop"),
            Map.entry(
                "index.analysis.analyzer.iq_text_delimiter.filter",
                List.of("delimiter", "cjk_width", "lowercase", "asciifolding", "en-stop-words-filter", "en-stem-filter")
            ),
            Map.entry("index.analysis.analyzer.iq_text_delimiter.tokenizer", "whitespace"),
            Map.entry("index.analysis.analyzer.q_prefix.filter", List.of("cjk_width", "lowercase", "asciifolding")),
            Map.entry("index.analysis.analyzer.q_prefix.tokenizer", "standard"),
            Map.entry(
                "index.analysis.analyzer.iq_text_base.filter",
                List.of("cjk_width", "lowercase", "asciifolding", "en-stop-words-filter")
            ),
            Map.entry("index.analysis.analyzer.iq_text_base.tokenizer", "standard"),
            Map.entry(
                "index.analysis.analyzer.iq_text_stem.filter",
                List.of("cjk_width", "lowercase", "asciifolding", "en-stop-words-filter", "en-stem-filter")
            ),
            Map.entry("index.analysis.analyzer.iq_text_stem.tokenizer", "standard"),
            Map.entry(
                "index.analysis.analyzer.i_text_bigram.filter",
                List.of("cjk_width", "lowercase", "asciifolding", "en-stem-filter", "bigram_joiner", "bigram_max_size")
            ),
            Map.entry("index.analysis.analyzer.i_text_bigram.tokenizer", "standard"),
            Map.entry(
                "index.analysis.analyzer.q_text_bigram.filter",
                List.of("cjk_width", "lowercase", "asciifolding", "en-stem-filter", "bigram_joiner_unigrams", "bigram_max_size")
            ),
            Map.entry("index.analysis.analyzer.q_text_bigram.tokenizer", "standard")
        );

        Settings.Builder index2SettingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put("index.mapping.total_fields.limit", 99999999)
            .put("index.mapping.depth.limit", 30)
            .put("index.mapping.nested_fields.limit", 40)
            .put("index.mapping.nested_objects.limit", 20000)
            .put("index.mapping.field_name_length.limit", 65536)
            .put("index.mapping.dimension_fields.limit", 32);
        index2SettingsBuilder = (expectedError == ExpectedError.INDEX_SIMILARITY)
            ? index2SettingsBuilder.put("index.similarity.default", "boolean")
            : index2SettingsBuilder.put("index.similarity.default", "bm25");
        index2SettingsBuilder.loadFromMap(analysisSettings2);
        Settings index2Settings = index2SettingsBuilder.build();

        ArgumentCaptor<GetSettingsRequest> getSettingsRequestCaptor = ArgumentCaptor.forClass(GetSettingsRequest.class);
        ArgumentCaptor<GetMappingsRequest> getMappingsRequestCaptor = ArgumentCaptor.forClass(GetMappingsRequest.class);
        ArgumentCaptor<FieldCapabilitiesRequest> fieldCapabilitiesRequestCaptor = ArgumentCaptor.forClass(FieldCapabilitiesRequest.class);

        Map<String, Settings> indexToSettings = Map.of("index_1", index1Settings, "index_2", index2Settings);

        GetSettingsResponse getSettingsResponse = new GetSettingsResponse(indexToSettings, Map.of());

        doAnswer(callListenerOnResponse(getSettingsResponse)).when(client)
            .execute(eq(GetSettingsAction.INSTANCE), getSettingsRequestCaptor.capture(), any());

        Map<String, Object> indexMappings = Map.of(
            "properties",
            Map.of(
                "field_1",
                "field_1_mappings",
                "field_2",
                "field_2_mappings",
                NUMERICAL_FIELD,
                Map.of("type", "integer"),
                OUTER_FIELD,
                Map.of("properties", Map.of(INNER_FIELD, Map.of("type", "integer"))),
                ALIAS_TO_NUMERICAL_FIELD,
                Map.of("type", "alias", "path", NUMERICAL_FIELD),
                ALIAS_TO_NESTED_FIELD,
                Map.of("type", "alias", "path", "outer-field.inner-field")
            )
        );
        MappingMetadata index1MappingMetadata = new MappingMetadata("_doc", indexMappings);
        MappingMetadata index2MappingMetadata = new MappingMetadata("_doc", indexMappings);

        Map<String, MappingMetadata> mappings = Map.of("index_1", index1MappingMetadata, "index_2", index2MappingMetadata);

        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings);

        doAnswer(callListenerOnResponse(getMappingsResponse)).when(client)
            .execute(eq(GetMappingsAction.INSTANCE), getMappingsRequestCaptor.capture(), any());

        FieldCapabilitiesResponse fieldCapabilitiesResponse = new FieldCapabilitiesResponse(new String[0], new HashMap<>() {
            {
                put(NUMERICAL_FIELD, singletonMap("integer", createFieldCapabilities(NUMERICAL_FIELD, "integer")));
                put(OUTER_FIELD + "." + INNER_FIELD, singletonMap("integer", createFieldCapabilities(NUMERICAL_FIELD, "integer")));
                put(ALIAS_TO_NUMERICAL_FIELD, singletonMap("integer", createFieldCapabilities(NUMERICAL_FIELD, "integer")));
                put(ALIAS_TO_NESTED_FIELD, singletonMap("integer", createFieldCapabilities(NUMERICAL_FIELD, "integer")));
            }
        });

        doAnswer(callListenerOnResponse(fieldCapabilitiesResponse)).when(client)
            .execute(eq(TransportFieldCapabilitiesAction.TYPE), fieldCapabilitiesRequestCaptor.capture(), any());

        String errorMessage = "";
        switch (expectedError) {
            case NONE: {
                break;
            }
            case INDEX_SIMILARITY: {
                errorMessage = "cannot merge settings because of differences for index\\.similarity; specified as "
                    + "\\[\\{\"default\":\"(bm25|boolean)\"}] in index \\[index_\\d]; specified as "
                    + "\\[\\{\"default\":\"(bm25|boolean)\"}] in index \\[index_\\d]";
                break;
            }
            case INDEX_ANALYSIS_FILTER: {
                errorMessage = "cannot merge settings because of differences for index\\.analysis\\.filter\\.en-stem-filter; specified as "
                    + "\\[\\{\"name\":\"light_english\",\"type\":\"(stemmer|foobarbaz)\"}] in index \\[index_\\d]; specified as"
                    + " \\[\\{\"name\":\"light_english\",\"type\":\"(stemmer|foobarbaz)\"}] in index \\[index_\\d]";
                break;
            }
            case INDEX_ANALYSIS_ANALYZER: {
                errorMessage = "cannot merge settings because of differences for "
                    + "index\\.analysis\\.analyzer\\.iq_text_delimiter; specified as "
                    + "\\[\\{\"filter\":\\[\"delimiter\",\"cjk_width\",\"lowercase\",(\"asciifolding\",)?"
                    + "\"en-stop-words-filter\",\"en-stem-filter\"],\"tokenizer\":\"whitespace\"}] in index \\[index_\\d]; specified as "
                    + "\\[\\{\"filter\":\\[\"delimiter\",\"cjk_width\",\"lowercase\",(\"asciifolding\",)?"
                    + "\"en-stop-words-filter\",\"en-stem-filter\"],\"tokenizer\":\"whitespace\"}] in index \\[index_\\d]";
                break;
            }
            default: {
                assertThat("Unexpected error case " + expectedError, Matchers.is(false));
                break;
            }
        }

        if (errorMessage.isEmpty() == false) {
            String finalErrorMessage = errorMessage;
            DestinationIndex.createDestinationIndex(
                client,
                clock,
                config,
                ANALYTICS_DEST_INDEX_ALLOWED_SETTINGS,
                assertNoSuccessListener(e -> assertThat(e.getMessage(), Matchers.matchesRegex(finalErrorMessage)))
            );

            return null;
        }

        DestinationIndex.createDestinationIndex(
            client,
            clock,
            config,
            ANALYTICS_DEST_INDEX_ALLOWED_SETTINGS,
            ActionTestUtils.assertNoFailureListener(response -> {})
        );

        GetSettingsRequest capturedGetSettingsRequest = getSettingsRequestCaptor.getValue();
        assertThat(capturedGetSettingsRequest.indices(), equalTo(SOURCE_INDEX));
        assertThat(capturedGetSettingsRequest.indicesOptions(), equalTo(IndicesOptions.lenientExpandOpen()));
        assertThat(
            Arrays.asList(capturedGetSettingsRequest.names()),
            contains("index.number_of_shards", "index.number_of_replicas", "index.analysis.*", "index.similarity.*", "index.mapping.*")
        );

        assertThat(getMappingsRequestCaptor.getValue().indices(), equalTo(SOURCE_INDEX));

        CreateIndexRequest createIndexRequest = createIndexRequestCaptor.getValue();

        assertThat(
            createIndexRequest.settings().keySet(),
            containsInAnyOrder(
                "index.number_of_shards",
                "index.number_of_replicas",
                "index.mapping.total_fields.limit",
                "index.mapping.depth.limit",
                "index.mapping.nested_fields.limit",
                "index.mapping.nested_objects.limit",
                "index.mapping.field_name_length.limit",
                "index.mapping.dimension_fields.limit",
                "index.similarity.default",
                "index.analysis.analyzer.i_prefix.filter",
                "index.analysis.analyzer.i_prefix.tokenizer",
                "index.analysis.analyzer.i_text_bigram.filter",
                "index.analysis.analyzer.i_text_bigram.tokenizer",
                "index.analysis.analyzer.iq_text_base.filter",
                "index.analysis.analyzer.iq_text_base.tokenizer",
                "index.analysis.analyzer.iq_text_delimiter.filter",
                "index.analysis.analyzer.iq_text_delimiter.tokenizer",
                "index.analysis.analyzer.iq_text_stem.filter",
                "index.analysis.analyzer.iq_text_stem.tokenizer",
                "index.analysis.analyzer.q_prefix.filter",
                "index.analysis.analyzer.q_prefix.tokenizer",
                "index.analysis.analyzer.q_text_bigram.filter",
                "index.analysis.analyzer.q_text_bigram.tokenizer",
                "index.analysis.filter.bigram_joiner.max_shingle_size",
                "index.analysis.filter.bigram_joiner.output_unigrams",
                "index.analysis.filter.bigram_joiner.token_separator",
                "index.analysis.filter.bigram_joiner.type",
                "index.analysis.filter.bigram_joiner_unigrams.max_shingle_size",
                "index.analysis.filter.bigram_joiner_unigrams.output_unigrams",
                "index.analysis.filter.bigram_joiner_unigrams.token_separator",
                "index.analysis.filter.bigram_joiner_unigrams.type",
                "index.analysis.filter.bigram_max_size.max",
                "index.analysis.filter.bigram_max_size.min",
                "index.analysis.filter.bigram_max_size.type",
                "index.analysis.filter.en-stem-filter.name",
                "index.analysis.filter.en-stem-filter.type",
                "index.analysis.filter.en-stop-words-filter.stopwords",
                "index.analysis.filter.en-stop-words-filter.type",
                "index.analysis.filter.front_ngram.max_gram",
                "index.analysis.filter.front_ngram.min_gram",
                "index.analysis.filter.front_ngram.type"
            )
        );

        assertThat(createIndexRequest.settings().getAsInt("index.number_of_shards", -1), equalTo(5));
        assertThat(createIndexRequest.settings().getAsInt("index.number_of_replicas", -1), equalTo(1));
        assertThat(createIndexRequest.settings().getAsLong("index.mapping.total_fields.limit", -1L), equalTo(99999999L));
        assertThat(createIndexRequest.settings().getAsLong("index.mapping.depth.limit", -1L), equalTo(30L));
        assertThat(createIndexRequest.settings().getAsLong("index.mapping.nested_fields.limit", -1L), equalTo(50L));
        assertThat(createIndexRequest.settings().getAsLong("index.mapping.nested_objects.limit", -1L), equalTo(20000L));
        assertThat(createIndexRequest.settings().getAsLong("index.mapping.field_name_length.limit", -1L), equalTo(Long.MAX_VALUE));
        assertThat(createIndexRequest.settings().getAsLong("index.mapping.dimension_fields.limit", -1L), equalTo(32L));
        assertThat(
            createIndexRequest.settings().getAsList("index.analysis.analyzer.i_prefix.filter"),
            equalTo(List.of("cjk_width", "lowercase", "asciifolding", "front_ngram"))
        );
        assertThat(createIndexRequest.settings().get("index.analysis.analyzer.i_prefix.tokenizer"), equalTo("standard"));
        assertThat(
            createIndexRequest.settings().getAsList("index.analysis.analyzer.iq_text_delimiter.filter"),
            equalTo(List.of("delimiter", "cjk_width", "lowercase", "asciifolding", "en-stop-words-filter", "en-stem-filter"))
        );
        assertThat(createIndexRequest.settings().get("index.analysis.analyzer.iq_text_delimiter.tokenizer"), equalTo("whitespace"));
        assertThat(
            createIndexRequest.settings().getAsList("index.analysis.analyzer.q_prefix.filter"),
            equalTo(List.of("cjk_width", "lowercase", "asciifolding"))
        );
        assertThat(createIndexRequest.settings().get("index.analysis.analyzer.q_prefix.tokenizer"), equalTo("standard"));
        assertThat(
            createIndexRequest.settings().getAsList("index.analysis.analyzer.iq_text_base.filter"),
            equalTo(List.of("cjk_width", "lowercase", "asciifolding", "en-stop-words-filter"))
        );
        assertThat(createIndexRequest.settings().get("index.analysis.analyzer.iq_text_base.tokenizer"), equalTo("standard"));
        assertThat(
            createIndexRequest.settings().getAsList("index.analysis.analyzer.iq_text_stem.filter"),
            equalTo(List.of("cjk_width", "lowercase", "asciifolding", "en-stop-words-filter", "en-stem-filter"))
        );
        assertThat(createIndexRequest.settings().get("index.analysis.analyzer.iq_text_stem.tokenizer"), equalTo("standard"));
        assertThat(
            createIndexRequest.settings().getAsList("index.analysis.analyzer.i_text_bigram.filter"),
            equalTo(List.of("cjk_width", "lowercase", "asciifolding", "en-stem-filter", "bigram_joiner", "bigram_max_size"))
        );
        assertThat(createIndexRequest.settings().get("index.analysis.analyzer.i_text_bigram.tokenizer"), equalTo("standard"));
        assertThat(
            createIndexRequest.settings().getAsList("index.analysis.analyzer.q_text_bigram.filter"),
            equalTo(List.of("cjk_width", "lowercase", "asciifolding", "en-stem-filter", "bigram_joiner_unigrams", "bigram_max_size"))
        );
        assertThat(createIndexRequest.settings().get("index.analysis.analyzer.q_text_bigram.tokenizer"), equalTo("standard"));

        assertThat(createIndexRequest.settings().getAsInt("index.analysis.filter.bigram_joiner.max_shingle_size", -1), equalTo(2));
        assertThat(createIndexRequest.settings().getAsBoolean("index.analysis.filter.bigram_joiner.output_unigrams", true), equalTo(false));
        assertThat(createIndexRequest.settings().get("index.analysis.filter.bigram_joiner.token_separator"), equalTo(""));
        assertThat(createIndexRequest.settings().get("index.analysis.filter.bigram_joiner.type"), equalTo("shingle"));

        assertThat(createIndexRequest.settings().getAsInt("index.analysis.filter.bigram_joiner_unigrams.max_shingle_size", -1), equalTo(2));
        assertThat(
            createIndexRequest.settings().getAsBoolean("index.analysis.filter.bigram_joiner_unigrams.output_unigrams", false),
            equalTo(true)
        );
        assertThat(createIndexRequest.settings().get("index.analysis.filter.bigram_joiner_unigrams.token_separator"), equalTo(""));
        assertThat(createIndexRequest.settings().get("index.analysis.filter.bigram_joiner_unigrams.type"), equalTo("shingle"));

        assertThat(createIndexRequest.settings().getAsInt("index.analysis.filter.bigram_max_size.max", -1), equalTo(16));
        assertThat(createIndexRequest.settings().getAsInt("index.analysis.filter.bigram_max_size.min", -1), equalTo(0));
        assertThat(createIndexRequest.settings().get("index.analysis.filter.bigram_max_size.type"), equalTo("length"));

        assertThat(createIndexRequest.settings().get("index.analysis.filter.en-stem-filter.name"), equalTo("light_english"));
        assertThat(createIndexRequest.settings().get("index.analysis.filter.en-stem-filter.type"), equalTo("stemmer"));

        assertThat(createIndexRequest.settings().get("index.analysis.filter.en-stop-words-filter.stopwords"), equalTo("_english_"));
        assertThat(createIndexRequest.settings().get("index.analysis.filter.en-stop-words-filter.type"), equalTo("stop"));

        assertThat(createIndexRequest.settings().getAsInt("index.analysis.filter.front_ngram.max_gram", -1), equalTo(12));
        assertThat(createIndexRequest.settings().getAsInt("index.analysis.filter.front_ngram.min_gram", -1), equalTo(1));
        assertThat(createIndexRequest.settings().get("index.analysis.filter.front_ngram.type"), equalTo("edge_ngram"));

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, createIndexRequest.mappings())) {
            Map<String, Object> map = parser.map();
            assertThat(extractValue("_doc.properties.ml__incremental_id.type", map), equalTo("long"));
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
        for (ExpectedError expectedError : ExpectedError.values()) {
            testCreateDestinationIndex(new OutlierDetection.Builder().build(), expectedError);
        }
    }

    public void testCreateDestinationIndex_Regression() throws IOException {
        for (ExpectedError expectedError : ExpectedError.values()) {
            Map<String, Object> map = testCreateDestinationIndex(new Regression(NUMERICAL_FIELD), expectedError);
            if (expectedError == ExpectedError.NONE) {
                assertThat(extractValue("_doc.properties.ml.numerical-field_prediction.type", map), equalTo("double"));
            } else {
                assertThat(map, equalTo(null));
            }
        }
    }

    public void testCreateDestinationIndex_Classification() throws IOException {
        for (ExpectedError expectedError : ExpectedError.values()) {
            Map<String, Object> map = testCreateDestinationIndex(new Classification(NUMERICAL_FIELD), expectedError);
            if (expectedError == ExpectedError.NONE) {
                assertThat(extractValue("_doc.properties.ml.numerical-field_prediction.type", map), equalTo("integer"));
                assertThat(extractValue("_doc.properties.ml.top_classes.properties.class_name.type", map), equalTo("integer"));
            } else {
                assertThat(map, equalTo(null));
            }
        }
    }

    public void testCreateDestinationIndex_Classification_DependentVariableIsNested() throws IOException {
        for (ExpectedError expectedError : ExpectedError.values()) {
            Map<String, Object> map = testCreateDestinationIndex(new Classification(OUTER_FIELD + "." + INNER_FIELD), expectedError);
            if (expectedError == ExpectedError.NONE) {
                assertThat(extractValue("_doc.properties.ml.outer-field.inner-field_prediction.type", map), equalTo("integer"));
                assertThat(extractValue("_doc.properties.ml.top_classes.properties.class_name.type", map), equalTo("integer"));
            } else {
                assertThat(map, equalTo(null));
            }
        }
    }

    public void testCreateDestinationIndex_Classification_DependentVariableIsAlias() throws IOException {
        for (ExpectedError expectedError : ExpectedError.values()) {
            Map<String, Object> map = testCreateDestinationIndex(new Classification(ALIAS_TO_NUMERICAL_FIELD), expectedError);
            if (expectedError == ExpectedError.NONE) {
                assertThat(extractValue("_doc.properties.ml.alias-to-numerical-field_prediction.type", map), equalTo("integer"));
                assertThat(extractValue("_doc.properties.ml.top_classes.properties.class_name.type", map), equalTo("integer"));
            } else {
                assertThat(map, equalTo(null));
            }
        }
    }

    public void testCreateDestinationIndex_Classification_DependentVariableIsAliasToNested() throws IOException {
        for (ExpectedError expectedError : ExpectedError.values()) {
            Map<String, Object> map = testCreateDestinationIndex(new Classification(ALIAS_TO_NESTED_FIELD), expectedError);
            if (expectedError == ExpectedError.NONE) {
                assertThat(extractValue("_doc.properties.ml.alias-to-nested-field_prediction.type", map), equalTo("integer"));
                assertThat(extractValue("_doc.properties.ml.top_classes.properties.class_name.type", map), equalTo("integer"));
            } else {
                assertThat(map, equalTo(null));
            }
        }
    }

    public void testCreateDestinationIndex_ResultsFieldsExistsInSourceIndex() {
        DataFrameAnalyticsConfig config = createConfig(new OutlierDetection.Builder().build());

        GetSettingsResponse getSettingsResponse = new GetSettingsResponse(Map.of(), Map.of());

        Map<String, MappingMetadata> mappings = Map.of("", new MappingMetadata("_doc", Map.of("properties", Map.of("ml", "some-mapping"))));
        GetMappingsResponse getMappingsResponse = new GetMappingsResponse(mappings);

        doAnswer(callListenerOnResponse(getSettingsResponse)).when(client).execute(eq(GetSettingsAction.INSTANCE), any(), any());
        doAnswer(callListenerOnResponse(getMappingsResponse)).when(client).execute(eq(GetMappingsAction.INSTANCE), any(), any());

        DestinationIndex.createDestinationIndex(
            client,
            clock,
            config,
            ANALYTICS_DEST_INDEX_ALLOWED_SETTINGS,
            assertNoSuccessListener(
                e -> assertThat(
                    e.getMessage(),
                    equalTo("A field that matches the dest.results_field [ml] already exists; please set a different results_field")
                )
            )
        );
    }

    private Map<String, Object> testUpdateMappingsToDestIndex(DataFrameAnalysis analysis) throws IOException {
        DataFrameAnalyticsConfig config = createConfig(analysis);

        Map<String, Object> properties = Map.of(
            NUMERICAL_FIELD,
            Map.of("type", "integer"),
            OUTER_FIELD,
            Map.of("properties", Map.of(INNER_FIELD, Map.of("type", "integer"))),
            ALIAS_TO_NUMERICAL_FIELD,
            Map.of("type", "alias", "path", NUMERICAL_FIELD),
            ALIAS_TO_NESTED_FIELD,
            Map.of("type", "alias", "path", OUTER_FIELD + "." + INNER_FIELD)
        );
        Map<String, MappingMetadata> mappings = Map.of("", new MappingMetadata("_doc", Map.of("properties", properties)));
        GetIndexResponse getIndexResponse = new GetIndexResponse(
            new String[] { DEST_INDEX },
            mappings,
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of()
        );

        ArgumentCaptor<PutMappingRequest> putMappingRequestCaptor = ArgumentCaptor.forClass(PutMappingRequest.class);
        ArgumentCaptor<FieldCapabilitiesRequest> fieldCapabilitiesRequestCaptor = ArgumentCaptor.forClass(FieldCapabilitiesRequest.class);

        doAnswer(callListenerOnResponse(AcknowledgedResponse.TRUE)).when(client)
            .execute(eq(TransportPutMappingAction.TYPE), putMappingRequestCaptor.capture(), any());

        FieldCapabilitiesResponse fieldCapabilitiesResponse = new FieldCapabilitiesResponse(new String[0], new HashMap<>() {
            {
                put(NUMERICAL_FIELD, singletonMap("integer", createFieldCapabilities(NUMERICAL_FIELD, "integer")));
                put(OUTER_FIELD + "." + INNER_FIELD, singletonMap("integer", createFieldCapabilities(NUMERICAL_FIELD, "integer")));
                put(ALIAS_TO_NUMERICAL_FIELD, singletonMap("integer", createFieldCapabilities(NUMERICAL_FIELD, "integer")));
                put(ALIAS_TO_NESTED_FIELD, singletonMap("integer", createFieldCapabilities(NUMERICAL_FIELD, "integer")));
            }
        });

        doAnswer(callListenerOnResponse(fieldCapabilitiesResponse)).when(client)
            .execute(eq(TransportFieldCapabilitiesAction.TYPE), fieldCapabilitiesRequestCaptor.capture(), any());

        DestinationIndex.updateMappingsToDestIndex(
            client,
            config,
            getIndexResponse,
            ActionTestUtils.assertNoFailureListener(response -> assertThat(response.isAcknowledged(), is(true)))
        );

        verify(client, atLeastOnce()).threadPool();
        verify(client, atMost(1)).execute(eq(TransportFieldCapabilitiesAction.TYPE), any(), any());
        verify(client).execute(eq(TransportPutMappingAction.TYPE), any(), any());
        verifyNoMoreInteractions(client);

        PutMappingRequest putMappingRequest = putMappingRequestCaptor.getValue();
        assertThat(putMappingRequest.indices(), arrayContaining(DEST_INDEX));
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, putMappingRequest.source())) {
            Map<String, Object> map = parser.map();
            assertThat(extractValue("properties.ml__incremental_id.type", map), equalTo("long"));
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
        assertThat(extractValue("properties.ml.top_classes.properties.class_name.type", map), equalTo("integer"));
    }

    public void testUpdateMappingsToDestIndex_Classification_DependentVariableIsNested() throws IOException {
        Map<String, Object> map = testUpdateMappingsToDestIndex(new Classification(OUTER_FIELD + "." + INNER_FIELD));
        assertThat(extractValue("properties.ml.outer-field.inner-field_prediction.type", map), equalTo("integer"));
        assertThat(extractValue("properties.ml.top_classes.properties.class_name.type", map), equalTo("integer"));
    }

    public void testUpdateMappingsToDestIndex_Classification_DependentVariableIsAlias() throws IOException {
        Map<String, Object> map = testUpdateMappingsToDestIndex(new Classification(ALIAS_TO_NUMERICAL_FIELD));
        assertThat(extractValue("properties.ml.alias-to-numerical-field_prediction.type", map), equalTo("integer"));
        assertThat(extractValue("properties.ml.top_classes.properties.class_name.type", map), equalTo("integer"));
    }

    public void testUpdateMappingsToDestIndex_Classification_DependentVariableIsAliasToNested() throws IOException {
        Map<String, Object> map = testUpdateMappingsToDestIndex(new Classification(ALIAS_TO_NESTED_FIELD));
        assertThat(extractValue("properties.ml.alias-to-nested-field_prediction.type", map), equalTo("integer"));
        assertThat(extractValue("properties.ml.top_classes.properties.class_name.type", map), equalTo("integer"));
    }

    public void testUpdateMappingsToDestIndex_ResultsFieldsExistsInSourceIndex() {
        DataFrameAnalyticsConfig config = createConfig(new OutlierDetection.Builder().build());

        Map<String, MappingMetadata> mappings = Map.of("", new MappingMetadata("_doc", Map.of("properties", Map.of("ml", "some-mapping"))));
        GetIndexResponse getIndexResponse = new GetIndexResponse(
            new String[] { DEST_INDEX },
            mappings,
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of()
        );

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> DestinationIndex.updateMappingsToDestIndex(client, config, getIndexResponse, ActionListener.running(Assert::fail))
        );
        assertThat(
            e.getMessage(),
            equalTo("A field that matches the dest.results_field [ml] already exists; please set a different results_field")
        );

        verifyNoMoreInteractions(client);
    }

    public void testReadMetadata_GivenNoMeta() {
        Map<String, Object> mappings = new HashMap<>();
        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.getSourceAsMap()).thenReturn(mappings);

        DestinationIndex.Metadata metadata = DestinationIndex.readMetadata("test_id", mappingMetadata);

        assertThat(metadata.hasMetadata(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> metadata.isCompatible());
        expectThrows(UnsupportedOperationException.class, () -> metadata.getVersion());
    }

    public void testReadMetadata_GivenMetaWithoutCreatedTag() {
        Map<String, Object> mappings = new HashMap<>();
        mappings.put("_meta", Collections.emptyMap());
        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.getSourceAsMap()).thenReturn(mappings);

        DestinationIndex.Metadata metadata = DestinationIndex.readMetadata("test_id", mappingMetadata);

        assertThat(metadata.hasMetadata(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> metadata.isCompatible());
        expectThrows(UnsupportedOperationException.class, () -> metadata.getVersion());
    }

    public void testReadMetadata_GivenMetaNotCreatedByAnalytics() {
        Map<String, Object> mappings = new HashMap<>();
        mappings.put("_meta", singletonMap("created", "other"));
        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.getSourceAsMap()).thenReturn(mappings);

        DestinationIndex.Metadata metadata = DestinationIndex.readMetadata("test_id", mappingMetadata);

        assertThat(metadata.hasMetadata(), is(false));
        expectThrows(UnsupportedOperationException.class, () -> metadata.isCompatible());
        expectThrows(UnsupportedOperationException.class, () -> metadata.getVersion());
    }

    public void testReadMetadata_GivenCurrentVersion() {
        Map<String, Object> mappings = new HashMap<>();
        mappings.put("_meta", DestinationIndex.createMetadata("test_id", Clock.systemUTC(), MlConfigVersion.CURRENT));
        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.getSourceAsMap()).thenReturn(mappings);

        DestinationIndex.Metadata metadata = DestinationIndex.readMetadata("test_id", mappingMetadata);

        assertThat(metadata.hasMetadata(), is(true));
        assertThat(metadata.isCompatible(), is(true));
        assertThat(metadata.getVersion(), equalTo(MlConfigVersion.CURRENT.toString()));
    }

    public void testReadMetadata_GivenMinCompatibleVersion() {
        Map<String, Object> mappings = new HashMap<>();
        mappings.put("_meta", DestinationIndex.createMetadata("test_id", Clock.systemUTC(), DestinationIndex.MIN_COMPATIBLE_VERSION));
        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.getSourceAsMap()).thenReturn(mappings);

        DestinationIndex.Metadata metadata = DestinationIndex.readMetadata("test_id", mappingMetadata);

        assertThat(metadata.hasMetadata(), is(true));
        assertThat(metadata.isCompatible(), is(true));
        assertThat(metadata.getVersion(), equalTo(DestinationIndex.MIN_COMPATIBLE_VERSION.toString()));
    }

    public void testReadMetadata_GivenIncompatibleVersion() {
        Map<String, Object> mappings = new HashMap<>();
        mappings.put("_meta", DestinationIndex.createMetadata("test_id", Clock.systemUTC(), MlConfigVersion.V_7_9_3));
        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.getSourceAsMap()).thenReturn(mappings);

        DestinationIndex.Metadata metadata = DestinationIndex.readMetadata("test_id", mappingMetadata);

        assertThat(metadata.hasMetadata(), is(true));
        assertThat(metadata.isCompatible(), is(false));
        assertThat(metadata.getVersion(), equalTo(MlConfigVersion.V_7_9_3.toString()));
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
        return new DataFrameAnalyticsConfig.Builder().setId(ANALYTICS_ID)
            .setSource(new DataFrameAnalyticsSource(SOURCE_INDEX, null, null, null))
            .setDest(new DataFrameAnalyticsDest(DEST_INDEX, null))
            .setAnalysis(analysis)
            .build();
    }

    private static FieldCapabilities createFieldCapabilities(String field, String type) {
        return new FieldCapabilitiesBuilder(field, type).build();
    }
}
