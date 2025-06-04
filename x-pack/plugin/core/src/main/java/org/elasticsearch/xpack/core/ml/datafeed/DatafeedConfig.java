/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;
import org.elasticsearch.xpack.core.ml.utils.RuntimeMappingsValidator;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.core.ml.utils.XContentObjectTransformer;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.ClientHelper.assertNoAuthorizationHeader;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.DATAFEED_AGGREGATIONS_COMPOSITE_AGG_DATE_HISTOGRAM_SORT;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.DATAFEED_AGGREGATIONS_COMPOSITE_AGG_DATE_HISTOGRAM_SOURCE_MISSING_BUCKET;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.DATAFEED_AGGREGATIONS_COMPOSITE_AGG_MUST_BE_TOP_LEVEL_AND_ALONE;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.DATAFEED_AGGREGATIONS_COMPOSITE_AGG_MUST_HAVE_SINGLE_DATE_SOURCE;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.DATAFEED_AGGREGATIONS_INTERVAL_MUST_BE_GREATER_THAN_ZERO;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.DATAFEED_AGGREGATIONS_MAX_ONE_DATE_HISTOGRAM;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.DATAFEED_AGGREGATIONS_REQUIRES_DATE_HISTOGRAM;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.DATAFEED_CONFIG_AGG_BAD_FORMAT;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.DATAFEED_CONFIG_CANNOT_USE_SCRIPT_FIELDS_WITH_AGGS;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.DATAFEED_CONFIG_INVALID_OPTION_VALUE;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.DATAFEED_CONFIG_QUERY_BAD_FORMAT;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.DATAFEED_DATA_HISTOGRAM_MUST_HAVE_NESTED_MAX_AGGREGATION;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.INVALID_ID;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.getMessage;
import static org.elasticsearch.xpack.core.ml.utils.ToXContentParams.EXCLUDE_GENERATED;

/**
 * Datafeed configuration options. Describes where to proactively pull input
 * data from.
 * <p>
 * If a value has not been set it will be <code>null</code>. Object wrappers are
 * used around integral types and booleans so they can take <code>null</code>
 * values.
 */
public class DatafeedConfig implements SimpleDiffable<DatafeedConfig>, ToXContentObject {

    public static final int DEFAULT_SCROLL_SIZE = 1000;

    private static final int SECONDS_IN_MINUTE = 60;
    private static final int TWO_MINS_SECONDS = 2 * SECONDS_IN_MINUTE;
    private static final int TWENTY_MINS_SECONDS = 20 * SECONDS_IN_MINUTE;
    private static final int HALF_DAY_SECONDS = 12 * 60 * SECONDS_IN_MINUTE;
    public static final int DEFAULT_AGGREGATION_CHUNKING_BUCKETS = 1000;
    private static final TimeValue MIN_DEFAULT_QUERY_DELAY = TimeValue.timeValueMinutes(1);
    private static final TimeValue MAX_DEFAULT_QUERY_DELAY = TimeValue.timeValueMinutes(2);

    private static final Logger logger = LogManager.getLogger(DatafeedConfig.class);

    // Used for QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("datafeeds");
    public static final String TYPE = "datafeed";

    /**
     * The field name used to specify document counts in Elasticsearch
     * aggregations
     */
    public static final String DOC_COUNT = "doc_count";

    // Accessing `Job.ID` here causes an NPE in tests as a DatafeedConfig parser is referenced in the Job parser
    public static final ParseField JOB_ID = new ParseField("job_id");
    public static final ParseField ID = new ParseField("datafeed_id");
    public static final ParseField CONFIG_TYPE = new ParseField("config_type");
    public static final ParseField QUERY_DELAY = new ParseField("query_delay");
    public static final ParseField FREQUENCY = new ParseField("frequency");
    public static final ParseField INDEXES = new ParseField("indexes");
    public static final ParseField INDICES = new ParseField("indices");
    public static final ParseField QUERY = new ParseField("query");
    public static final ParseField SCROLL_SIZE = new ParseField("scroll_size");
    public static final ParseField AGGREGATIONS = new ParseField("aggregations");
    public static final ParseField AGGS = new ParseField("aggs");
    public static final ParseField SCRIPT_FIELDS = new ParseField("script_fields");
    public static final ParseField CHUNKING_CONFIG = new ParseField("chunking_config");
    public static final ParseField HEADERS = new ParseField("headers");
    public static final ParseField DELAYED_DATA_CHECK_CONFIG = new ParseField("delayed_data_check_config");
    public static final ParseField MAX_EMPTY_SEARCHES = new ParseField("max_empty_searches");
    public static final ParseField INDICES_OPTIONS = new ParseField("indices_options");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ObjectParser<Builder, Void> LENIENT_PARSER = createParser(true);
    public static final ObjectParser<Builder, Void> STRICT_PARSER = createParser(false);

    public static void validateAggregations(AggregatorFactories.Builder aggregations) {
        if (aggregations == null) {
            return;
        }
        Collection<AggregationBuilder> aggregatorFactories = aggregations.getAggregatorFactories();
        if (aggregatorFactories.isEmpty()) {
            throw ExceptionsHelper.badRequestException(DATAFEED_AGGREGATIONS_REQUIRES_DATE_HISTOGRAM);
        }

        Builder.checkForOnlySingleTopLevelCompositeAggAndValidate(aggregations.getAggregatorFactories());
        AggregationBuilder histogramAggregation = DatafeedConfigUtils.getHistogramAggregation(aggregatorFactories);
        if (histogramAggregation instanceof CompositeAggregationBuilder
            && aggregations.getPipelineAggregatorFactories().isEmpty() == false) {
            throw ExceptionsHelper.badRequestException(
                "when using composite aggregations, top level pipeline aggregations are not supported"
            );
        }
        Builder.checkNoMoreHistogramAggregations(histogramAggregation.getSubAggregations());
        Builder.checkNoMoreCompositeAggregations(histogramAggregation.getSubAggregations());
        Builder.checkHistogramAggregationHasChildMaxTimeAgg(histogramAggregation);
        Builder.checkHistogramIntervalIsPositive(histogramAggregation);
    }

    private static ObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, Void> parser = new ObjectParser<>("datafeed_config", ignoreUnknownFields, Builder::new);

        parser.declareString(Builder::setId, ID);
        parser.declareString((c, s) -> {}, CONFIG_TYPE);
        parser.declareString(Builder::setJobId, JOB_ID);
        parser.declareStringArray(Builder::setIndices, INDEXES);
        parser.declareStringArray(Builder::setIndices, INDICES);
        parser.declareString(
            (builder, val) -> builder.setQueryDelay(TimeValue.parseTimeValue(val, QUERY_DELAY.getPreferredName())),
            QUERY_DELAY
        );
        parser.declareString(
            (builder, val) -> builder.setFrequency(TimeValue.parseTimeValue(val, FREQUENCY.getPreferredName())),
            FREQUENCY
        );
        parser.declareObject(
            Builder::setQueryProvider,
            (p, c) -> QueryProvider.fromXContent(p, ignoreUnknownFields, DATAFEED_CONFIG_QUERY_BAD_FORMAT),
            QUERY
        );
        parser.declareObject(Builder::setAggregationsSafe, (p, c) -> AggProvider.fromXContent(p, ignoreUnknownFields), AGGREGATIONS);
        parser.declareObject(Builder::setAggregationsSafe, (p, c) -> AggProvider.fromXContent(p, ignoreUnknownFields), AGGS);
        parser.declareObject(Builder::setScriptFields, (p, c) -> {
            List<SearchSourceBuilder.ScriptField> parsedScriptFields = new ArrayList<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                parsedScriptFields.add(new SearchSourceBuilder.ScriptField(p));
            }
            parsedScriptFields.sort(Comparator.comparing(SearchSourceBuilder.ScriptField::fieldName));
            return parsedScriptFields;
        }, SCRIPT_FIELDS);
        parser.declareInt(Builder::setScrollSize, SCROLL_SIZE);
        parser.declareObject(
            Builder::setChunkingConfig,
            ignoreUnknownFields ? ChunkingConfig.LENIENT_PARSER : ChunkingConfig.STRICT_PARSER,
            CHUNKING_CONFIG
        );

        if (ignoreUnknownFields) {
            // Headers are not parsed by the strict (config) parser, so headers supplied in the _body_ of a REST request will be rejected.
            // (For config, headers are explicitly transferred from the auth headers by code in the put/update datafeed actions.)
            parser.declareObject(Builder::setHeaders, (p, c) -> p.mapStrings(), HEADERS);
        }
        parser.declareObject(
            Builder::setDelayedDataCheckConfig,
            ignoreUnknownFields ? DelayedDataCheckConfig.LENIENT_PARSER : DelayedDataCheckConfig.STRICT_PARSER,
            DELAYED_DATA_CHECK_CONFIG
        );
        parser.declareInt(Builder::setMaxEmptySearches, MAX_EMPTY_SEARCHES);
        parser.declareObject(
            Builder::setIndicesOptions,
            (p, c) -> IndicesOptions.fromMap(p.map(), SearchRequest.DEFAULT_INDICES_OPTIONS),
            INDICES_OPTIONS
        );
        parser.declareObject(Builder::setRuntimeMappings, (p, c) -> p.map(), SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD);
        return parser;
    }

    private final String id;
    private final String jobId;

    /**
     * The delay before starting to query a period of time
     */
    private final TimeValue queryDelay;

    /**
     * The frequency with which queries are executed
     */
    private final TimeValue frequency;

    private final List<String> indices;
    private final QueryProvider queryProvider;
    private final AggProvider aggProvider;
    private final List<SearchSourceBuilder.ScriptField> scriptFields;
    private final Integer scrollSize;
    private final ChunkingConfig chunkingConfig;
    private final Map<String, String> headers;
    private final DelayedDataCheckConfig delayedDataCheckConfig;
    private final Integer maxEmptySearches;
    private final IndicesOptions indicesOptions;
    private final Map<String, Object> runtimeMappings;

    private DatafeedConfig(
        String id,
        String jobId,
        TimeValue queryDelay,
        TimeValue frequency,
        List<String> indices,
        QueryProvider queryProvider,
        AggProvider aggProvider,
        List<SearchSourceBuilder.ScriptField> scriptFields,
        Integer scrollSize,
        ChunkingConfig chunkingConfig,
        Map<String, String> headers,
        DelayedDataCheckConfig delayedDataCheckConfig,
        Integer maxEmptySearches,
        IndicesOptions indicesOptions,
        Map<String, Object> runtimeMappings
    ) {
        this.id = id;
        this.jobId = jobId;
        this.queryDelay = queryDelay;
        this.frequency = frequency;
        this.indices = indices == null ? null : Collections.unmodifiableList(indices);
        this.queryProvider = queryProvider == null ? null : new QueryProvider(queryProvider);
        this.aggProvider = aggProvider == null ? null : new AggProvider(aggProvider);
        this.scriptFields = scriptFields == null ? null : Collections.unmodifiableList(scriptFields);
        this.scrollSize = scrollSize;
        this.chunkingConfig = chunkingConfig;
        this.headers = Collections.unmodifiableMap(headers);
        this.delayedDataCheckConfig = delayedDataCheckConfig;
        this.maxEmptySearches = maxEmptySearches;
        this.indicesOptions = ExceptionsHelper.requireNonNull(indicesOptions, INDICES_OPTIONS);
        this.runtimeMappings = Collections.unmodifiableMap(runtimeMappings);
    }

    public DatafeedConfig(StreamInput in) throws IOException {
        this.id = in.readString();
        this.jobId = in.readString();
        this.queryDelay = in.readOptionalTimeValue();
        this.frequency = in.readOptionalTimeValue();
        if (in.readBoolean()) {
            this.indices = in.readCollectionAsImmutableList(StreamInput::readString);
        } else {
            this.indices = null;
        }
        // each of these writables are version aware
        this.queryProvider = QueryProvider.fromStream(in);
        // This reads a boolean from the stream, if true, it sends the stream to the `fromStream` method
        this.aggProvider = in.readOptionalWriteable(AggProvider::fromStream);

        if (in.readBoolean()) {
            this.scriptFields = in.readCollectionAsImmutableList(SearchSourceBuilder.ScriptField::new);
        } else {
            this.scriptFields = null;
        }
        this.scrollSize = in.readOptionalVInt();
        this.chunkingConfig = in.readOptionalWriteable(ChunkingConfig::new);
        this.headers = in.readImmutableMap(StreamInput::readString);
        delayedDataCheckConfig = in.readOptionalWriteable(DelayedDataCheckConfig::new);
        maxEmptySearches = in.readOptionalVInt();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        runtimeMappings = in.readGenericMap();
    }

    /**
     * The name of datafeed configuration document name from the datafeed ID.
     *
     * @param datafeedId The datafeed ID
     * @return The ID of document the datafeed config is persisted in
     */
    public static String documentId(String datafeedId) {
        return TYPE + "-" + datafeedId;
    }

    public String getId() {
        return id;
    }

    public String getJobId() {
        return jobId;
    }

    public String getConfigType() {
        return TYPE;
    }

    public TimeValue getQueryDelay() {
        return queryDelay;
    }

    public TimeValue getFrequency() {
        return frequency;
    }

    public List<String> getIndices() {
        return indices;
    }

    public Integer getScrollSize() {
        return scrollSize;
    }

    public Optional<Tuple<TransportVersion, String>> minRequiredTransportVersion() {
        return Optional.empty();
    }

    /**
     * Get the fully parsed query from the semi-parsed stored {@code Map<String, Object>}
     *
     * @param namedXContentRegistry XContent registry to transform the lazily parsed query
     * @return Fully parsed query
     */
    public QueryBuilder getParsedQuery(NamedXContentRegistry namedXContentRegistry) {
        return queryProvider == null ? null : parseQuery(namedXContentRegistry, new ArrayList<>());
    }

    // TODO Remove in v8.0.0
    // We only need this NamedXContentRegistry object if getParsedQuery() == null and getParsingException() == null
    // This situation only occurs in past versions that contained the lazy parsing support but not the providers (6.6.x)
    // We will still need `NamedXContentRegistry` for getting deprecations, but that is a special situation
    private QueryBuilder parseQuery(NamedXContentRegistry namedXContentRegistry, List<String> deprecations) {
        try {
            return queryProvider == null || queryProvider.getQuery() == null
                ? null
                : XContentObjectTransformer.queryBuilderTransformer(namedXContentRegistry).fromMap(queryProvider.getQuery(), deprecations);
        } catch (Exception exception) {
            // Certain thrown exceptions wrap up the real Illegal argument making it hard to determine cause for the user
            if (exception.getCause() instanceof IllegalArgumentException) {
                exception = (Exception) exception.getCause();
            }
            throw ExceptionsHelper.badRequestException(DATAFEED_CONFIG_QUERY_BAD_FORMAT, exception);
        }
    }

    Exception getQueryParsingException() {
        return queryProvider == null ? null : queryProvider.getParsingException();
    }

    /**
     * Calls the parser and returns any gathered deprecations
     *
     * @param namedXContentRegistry XContent registry to transform the lazily parsed query
     * @return The deprecations from parsing the query
     */
    public List<String> getQueryDeprecations(NamedXContentRegistry namedXContentRegistry) {
        List<String> deprecations = new ArrayList<>();
        parseQuery(namedXContentRegistry, deprecations);
        return deprecations;
    }

    public Map<String, Object> getQuery() {
        return queryProvider == null ? null : queryProvider.getQuery();
    }

    /**
     * Fully parses the semi-parsed {@code Map<String, Object>} aggregations
     *
     * @param namedXContentRegistry XContent registry to transform the lazily parsed aggregations
     * @return The fully parsed aggregations
     */
    public AggregatorFactories.Builder getParsedAggregations(NamedXContentRegistry namedXContentRegistry) {
        return aggProvider == null ? null : parseAggregations(namedXContentRegistry, new ArrayList<>());
    }

    // TODO refactor in v8.0.0
    // We only need this NamedXContentRegistry object if getParsedQuery() == null and getParsingException() == null
    // This situation only occurs in past versions that contained the lazy parsing support but not the providers (6.6.x)
    // We will still need `NamedXContentRegistry` for getting deprecations, but that is a special situation
    private AggregatorFactories.Builder parseAggregations(NamedXContentRegistry namedXContentRegistry, List<String> deprecations) {
        try {
            return aggProvider == null || aggProvider.getAggs() == null
                ? null
                : XContentObjectTransformer.aggregatorTransformer(namedXContentRegistry).fromMap(aggProvider.getAggs(), deprecations);
        } catch (Exception exception) {
            // Certain thrown exceptions wrap up the real Illegal argument making it hard to determine cause for the user
            if (exception.getCause() instanceof IllegalArgumentException) {
                exception = (Exception) exception.getCause();
            }
            throw ExceptionsHelper.badRequestException(DATAFEED_CONFIG_AGG_BAD_FORMAT, exception);
        }
    }

    Exception getAggParsingException() {
        return aggProvider == null ? null : aggProvider.getParsingException();
    }

    /**
     * Calls the parser and returns any gathered deprecations
     *
     * @param namedXContentRegistry XContent registry to transform the lazily parsed aggregations
     * @return The deprecations from parsing the aggregations
     */
    public List<String> getAggDeprecations(NamedXContentRegistry namedXContentRegistry) {
        List<String> deprecations = new ArrayList<>();
        parseAggregations(namedXContentRegistry, deprecations);
        return deprecations;
    }

    public Map<String, Object> getAggregations() {
        return aggProvider == null ? null : aggProvider.getAggs();
    }

    /**
     * Returns the histogram's interval as epoch millis.
     *
     * @param namedXContentRegistry XContent registry to transform the lazily parsed aggregations
     */
    public long getHistogramIntervalMillis(NamedXContentRegistry namedXContentRegistry) {
        return DatafeedConfigUtils.getHistogramIntervalMillis(getParsedAggregations(namedXContentRegistry));
    }

    /**
     * Indicates if the datafeed is using composite aggs.
     * @param namedXContentRegistry XContent registry to transform the lazily parsed aggregations
     * @return If the datafeed utilizes composite aggs or not
     */
    public boolean hasCompositeAgg(NamedXContentRegistry namedXContentRegistry) {
        if (hasAggregations() == false) {
            return false;
        }
        AggregationBuilder maybeComposite = DatafeedConfigUtils.getHistogramAggregation(
            getParsedAggregations(namedXContentRegistry).getAggregatorFactories()
        );
        return maybeComposite instanceof CompositeAggregationBuilder;
    }

    /**
     * @return {@code true} when there are non-empty aggregations, {@code false} otherwise
     */
    public boolean hasAggregations() {
        return aggProvider != null && aggProvider.getAggs() != null && aggProvider.getAggs().size() > 0;
    }

    public boolean aggsRewritten() {
        return aggProvider != null && aggProvider.isRewroteAggs();
    }

    public AggProvider getAggProvider() {
        return aggProvider;
    }

    public List<SearchSourceBuilder.ScriptField> getScriptFields() {
        return scriptFields == null ? Collections.emptyList() : scriptFields;
    }

    public ChunkingConfig getChunkingConfig() {
        return chunkingConfig;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public DelayedDataCheckConfig getDelayedDataCheckConfig() {
        return delayedDataCheckConfig;
    }

    public Integer getMaxEmptySearches() {
        return maxEmptySearches;
    }

    public IndicesOptions getIndicesOptions() {
        return indicesOptions;
    }

    public Map<String, Object> getRuntimeMappings() {
        return runtimeMappings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(jobId);
        out.writeOptionalTimeValue(queryDelay);
        out.writeOptionalTimeValue(frequency);
        if (indices != null) {
            out.writeBoolean(true);
            out.writeStringCollection(indices);
        } else {
            out.writeBoolean(false);
        }

        // Each of these writables are version aware
        queryProvider.writeTo(out); // never null
        // This writes a boolean to the stream, if true, it sends the stream to the `writeTo` method
        out.writeOptionalWriteable(aggProvider);

        if (scriptFields != null) {
            out.writeBoolean(true);
            out.writeCollection(scriptFields);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalVInt(scrollSize);
        out.writeOptionalWriteable(chunkingConfig);
        out.writeMap(headers, StreamOutput::writeString);
        out.writeOptionalWriteable(delayedDataCheckConfig);
        out.writeOptionalVInt(maxEmptySearches);
        indicesOptions.writeIndicesOptions(out);
        out.writeGenericMap(runtimeMappings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        final boolean forInternalStorage = params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false);
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.field(JOB_ID.getPreferredName(), jobId);
        if (params.paramAsBoolean(EXCLUDE_GENERATED, false) == false) {
            if (forInternalStorage) {
                builder.field(CONFIG_TYPE.getPreferredName(), TYPE);
            }
            if (headers.isEmpty() == false) {
                if (forInternalStorage) {
                    assertNoAuthorizationHeader(headers);
                    builder.field(HEADERS.getPreferredName(), headers);
                } else {
                    XContentUtils.addAuthorizationInfo(builder, headers);
                }
            }
            builder.field(QUERY_DELAY.getPreferredName(), queryDelay.getStringRep());
            if (chunkingConfig != null) {
                builder.field(CHUNKING_CONFIG.getPreferredName(), chunkingConfig);
            }
            builder.startObject(INDICES_OPTIONS.getPreferredName());
            indicesOptions.toXContent(builder, params);
            builder.endObject();
        } else { // Don't include random defaults or unnecessary defaults in export
            if (queryDelay.equals(defaultRandomQueryDelay(jobId)) == false) {
                builder.field(QUERY_DELAY.getPreferredName(), queryDelay.getStringRep());
            }
            // Indices options are a pretty advanced feature, better to not include them if they are just the default ones
            if (indicesOptions.equals(SearchRequest.DEFAULT_INDICES_OPTIONS) == false) {
                builder.startObject(INDICES_OPTIONS.getPreferredName());
                indicesOptions.toXContent(builder, params);
                builder.endObject();
            }
            // Removing the default chunking config as it is determined by OTHER fields
            if (chunkingConfig != null && chunkingConfig.equals(defaultChunkingConfig(aggProvider)) == false) {
                builder.field(CHUNKING_CONFIG.getPreferredName(), chunkingConfig);
            }
        }
        builder.field(QUERY.getPreferredName(), queryProvider.getQuery());
        if (frequency != null) {
            builder.field(FREQUENCY.getPreferredName(), frequency.getStringRep());
        }
        builder.field(INDICES.getPreferredName(), indices);
        if (aggProvider != null) {
            builder.field(AGGREGATIONS.getPreferredName(), aggProvider.getAggs());
        }
        if (scriptFields != null) {
            builder.startObject(SCRIPT_FIELDS.getPreferredName());
            for (SearchSourceBuilder.ScriptField scriptField : scriptFields) {
                scriptField.toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.field(SCROLL_SIZE.getPreferredName(), scrollSize);
        if (delayedDataCheckConfig != null) {
            builder.field(DELAYED_DATA_CHECK_CONFIG.getPreferredName(), delayedDataCheckConfig);
        }
        if (maxEmptySearches != null) {
            builder.field(MAX_EMPTY_SEARCHES.getPreferredName(), maxEmptySearches);
        }
        if (runtimeMappings.isEmpty() == false) {
            builder.field(SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD.getPreferredName(), runtimeMappings);
        }
        builder.endObject();
        return builder;
    }

    private static TimeValue defaultRandomQueryDelay(String jobId) {
        Random random = new Random(jobId.hashCode());
        long delayMillis = random.longs(MIN_DEFAULT_QUERY_DELAY.millis(), MAX_DEFAULT_QUERY_DELAY.millis()).findFirst().getAsLong();
        return TimeValue.timeValueMillis(delayMillis);
    }

    private static ChunkingConfig defaultChunkingConfig(@Nullable AggProvider aggProvider) {
        if (aggProvider == null || aggProvider.getParsedAggs() == null) {
            return ChunkingConfig.newAuto();
        } else {
            AggregationBuilder histogram = DatafeedConfigUtils.getHistogramAggregation(
                aggProvider.getParsedAggs().getAggregatorFactories()
            );
            if (histogram instanceof CompositeAggregationBuilder) {
                // Allow composite aggs to handle the underlying chunking and searching
                return ChunkingConfig.newOff();
            }
            long histogramIntervalMillis = DatafeedConfigUtils.getHistogramIntervalMillis(histogram);
            if (histogramIntervalMillis <= 0) {
                throw ExceptionsHelper.badRequestException(DATAFEED_AGGREGATIONS_INTERVAL_MUST_BE_GREATER_THAN_ZERO);
            }
            return ChunkingConfig.newManual(TimeValue.timeValueMillis(DEFAULT_AGGREGATION_CHUNKING_BUCKETS * histogramIntervalMillis));
        }
    }

    /**
     * The lists of indices and types are compared for equality but they are not
     * sorted first so this test could fail simply because the indices and types
     * lists are in different orders.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof DatafeedConfig == false) {
            return false;
        }

        DatafeedConfig that = (DatafeedConfig) other;

        return Objects.equals(this.id, that.id)
            && Objects.equals(this.jobId, that.jobId)
            && Objects.equals(this.frequency, that.frequency)
            && Objects.equals(this.queryDelay, that.queryDelay)
            && Objects.equals(this.indices, that.indices)
            && Objects.equals(this.queryProvider, that.queryProvider)
            && Objects.equals(this.scrollSize, that.scrollSize)
            && Objects.equals(this.aggProvider, that.aggProvider)
            && Objects.equals(this.scriptFields, that.scriptFields)
            && Objects.equals(this.chunkingConfig, that.chunkingConfig)
            && Objects.equals(this.headers, that.headers)
            && Objects.equals(this.delayedDataCheckConfig, that.delayedDataCheckConfig)
            && Objects.equals(this.maxEmptySearches, that.maxEmptySearches)
            && Objects.equals(this.indicesOptions, that.indicesOptions)
            && Objects.equals(this.runtimeMappings, that.runtimeMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            id,
            jobId,
            frequency,
            queryDelay,
            indices,
            queryProvider,
            scrollSize,
            aggProvider,
            scriptFields,
            chunkingConfig,
            headers,
            delayedDataCheckConfig,
            maxEmptySearches,
            indicesOptions,
            runtimeMappings
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Calculates a sensible default frequency for a given bucket span.
     * <p>
     * The default depends on the bucket span:
     * <ul>
     * <li> &lt;= 2 mins -&gt; 1 min</li>
     * <li> &lt;= 20 mins -&gt; bucket span / 2</li>
     * <li> &lt;= 12 hours -&gt; 10 mins</li>
     * <li> &gt; 12 hours -&gt; 1 hour</li>
     * </ul>
     *
     * If the datafeed has aggregations, the default frequency is the
     * closest multiple of the histogram interval based on the rules above.
     *
     * @param bucketSpan the bucket span
     * @return the default frequency
     */
    public TimeValue defaultFrequency(TimeValue bucketSpan, NamedXContentRegistry xContentRegistry) {
        TimeValue defaultFrequency = defaultFrequencyTarget(bucketSpan);
        if (hasAggregations()) {
            long histogramIntervalMillis = getHistogramIntervalMillis(xContentRegistry);
            long targetFrequencyMillis = defaultFrequency.millis();
            long defaultFrequencyMillis = histogramIntervalMillis > targetFrequencyMillis
                ? histogramIntervalMillis
                : (targetFrequencyMillis / histogramIntervalMillis) * histogramIntervalMillis;
            defaultFrequency = TimeValue.timeValueMillis(defaultFrequencyMillis);
        }
        return defaultFrequency;
    }

    private static TimeValue defaultFrequencyTarget(TimeValue bucketSpan) {
        long bucketSpanSeconds = bucketSpan.seconds();
        if (bucketSpanSeconds <= 0) {
            throw new IllegalArgumentException("Bucket span has to be > 0");
        }

        if (bucketSpanSeconds <= TWO_MINS_SECONDS) {
            return TimeValue.timeValueSeconds(SECONDS_IN_MINUTE);
        }
        if (bucketSpanSeconds <= TWENTY_MINS_SECONDS) {
            return TimeValue.timeValueSeconds(bucketSpanSeconds / 2);
        }
        if (bucketSpanSeconds <= HALF_DAY_SECONDS) {
            return TimeValue.timeValueMinutes(10);
        }
        return TimeValue.timeValueHours(1);
    }

    public static class Builder implements Writeable {

        private String id;
        private String jobId;
        private TimeValue queryDelay;
        private TimeValue frequency;
        private List<String> indices = Collections.emptyList();
        private QueryProvider queryProvider = QueryProvider.defaultQuery();
        private AggProvider aggProvider;
        private List<SearchSourceBuilder.ScriptField> scriptFields;
        private Integer scrollSize = DEFAULT_SCROLL_SIZE;
        private ChunkingConfig chunkingConfig;
        private Map<String, String> headers = Collections.emptyMap();
        private DelayedDataCheckConfig delayedDataCheckConfig = DelayedDataCheckConfig.defaultDelayedDataCheckConfig();
        private Integer maxEmptySearches;
        private IndicesOptions indicesOptions;
        private Map<String, Object> runtimeMappings = Collections.emptyMap();

        public Builder() {}

        public Builder(String id, String jobId) {
            this();
            this.id = ExceptionsHelper.requireNonNull(id, ID.getPreferredName());
            this.jobId = ExceptionsHelper.requireNonNull(jobId, JOB_ID.getPreferredName());
        }

        public Builder(DatafeedConfig config) {
            this.id = config.id;
            this.jobId = config.jobId;
            this.queryDelay = config.queryDelay;
            this.frequency = config.frequency;
            this.indices = new ArrayList<>(config.indices);
            this.queryProvider = config.queryProvider == null ? null : new QueryProvider(config.queryProvider);
            this.aggProvider = config.aggProvider == null ? null : new AggProvider(config.aggProvider);
            this.scriptFields = config.scriptFields == null ? null : new ArrayList<>(config.scriptFields);
            this.scrollSize = config.scrollSize;
            this.chunkingConfig = config.chunkingConfig;
            this.headers = new HashMap<>(config.headers);
            this.delayedDataCheckConfig = config.getDelayedDataCheckConfig();
            this.maxEmptySearches = config.getMaxEmptySearches();
            this.indicesOptions = config.indicesOptions;
            this.runtimeMappings = new HashMap<>(config.runtimeMappings);
        }

        public Builder(StreamInput in) throws IOException {
            this.id = in.readOptionalString();
            this.jobId = in.readOptionalString();
            this.queryDelay = in.readOptionalTimeValue();
            this.frequency = in.readOptionalTimeValue();
            if (in.readBoolean()) {
                this.indices = in.readCollectionAsImmutableList(StreamInput::readString);
            } else {
                this.indices = null;
            }
            // each of these writables are version aware
            this.queryProvider = QueryProvider.fromStream(in);
            // This reads a boolean from the stream, if true, it sends the stream to the `fromStream` method
            this.aggProvider = in.readOptionalWriteable(AggProvider::fromStream);

            if (in.readBoolean()) {
                this.scriptFields = in.readCollectionAsImmutableList(SearchSourceBuilder.ScriptField::new);
            } else {
                this.scriptFields = null;
            }
            this.scrollSize = in.readOptionalVInt();
            this.chunkingConfig = in.readOptionalWriteable(ChunkingConfig::new);
            this.headers = in.readImmutableMap(StreamInput::readString);
            delayedDataCheckConfig = in.readOptionalWriteable(DelayedDataCheckConfig::new);
            maxEmptySearches = in.readOptionalVInt();
            if (in.readBoolean()) {
                indicesOptions = IndicesOptions.readIndicesOptions(in);
            }
            runtimeMappings = in.readGenericMap();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(id);
            out.writeOptionalString(jobId);
            out.writeOptionalTimeValue(queryDelay);
            out.writeOptionalTimeValue(frequency);
            if (indices != null) {
                out.writeBoolean(true);
                out.writeStringCollection(indices);
            } else {
                out.writeBoolean(false);
            }

            // Each of these writables are version aware
            queryProvider.writeTo(out); // never null
            // This writes a boolean to the stream, if true, it sends the stream to the `writeTo` method
            out.writeOptionalWriteable(aggProvider);

            if (scriptFields != null) {
                out.writeBoolean(true);
                out.writeCollection(scriptFields);
            } else {
                out.writeBoolean(false);
            }
            out.writeOptionalVInt(scrollSize);
            out.writeOptionalWriteable(chunkingConfig);
            out.writeMap(headers, StreamOutput::writeString);
            out.writeOptionalWriteable(delayedDataCheckConfig);
            out.writeOptionalVInt(maxEmptySearches);
            out.writeBoolean(indicesOptions != null);
            if (indicesOptions != null) {
                indicesOptions.writeIndicesOptions(out);
            }
            out.writeGenericMap(runtimeMappings);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Builder builder = (Builder) o;
            return Objects.equals(id, builder.id)
                && Objects.equals(jobId, builder.jobId)
                && Objects.equals(queryDelay, builder.queryDelay)
                && Objects.equals(frequency, builder.frequency)
                && Objects.equals(indices, builder.indices)
                && Objects.equals(queryProvider, builder.queryProvider)
                && Objects.equals(aggProvider, builder.aggProvider)
                && Objects.equals(scriptFields, builder.scriptFields)
                && Objects.equals(scrollSize, builder.scrollSize)
                && Objects.equals(chunkingConfig, builder.chunkingConfig)
                && Objects.equals(headers, builder.headers)
                && Objects.equals(delayedDataCheckConfig, builder.delayedDataCheckConfig)
                && Objects.equals(maxEmptySearches, builder.maxEmptySearches)
                && Objects.equals(indicesOptions, builder.indicesOptions)
                && Objects.equals(runtimeMappings, builder.runtimeMappings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                id,
                jobId,
                queryDelay,
                frequency,
                indices,
                queryProvider,
                aggProvider,
                scriptFields,
                scrollSize,
                chunkingConfig,
                headers,
                delayedDataCheckConfig,
                maxEmptySearches,
                indicesOptions,
                runtimeMappings
            );
        }

        public Builder setId(String datafeedId) {
            id = ExceptionsHelper.requireNonNull(datafeedId, ID.getPreferredName());
            return this;
        }

        public String getId() {
            return id;
        }

        public Builder setJobId(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, JOB_ID.getPreferredName());
            return this;
        }

        public String getJobId() {
            return jobId;
        }

        public Builder setHeaders(Map<String, String> headers) {
            this.headers = ExceptionsHelper.requireNonNull(headers, HEADERS.getPreferredName());
            return this;
        }

        public Builder setIndices(List<String> indices) {
            this.indices = ExceptionsHelper.requireNonNull(indices, INDICES.getPreferredName());
            return this;
        }

        public Builder setQueryDelay(TimeValue queryDelay) {
            TimeUtils.checkNonNegativeMultiple(queryDelay, TimeUnit.MILLISECONDS, QUERY_DELAY);
            this.queryDelay = queryDelay;
            return this;
        }

        public Builder setFrequency(TimeValue frequency) {
            TimeUtils.checkPositiveMultiple(frequency, TimeUnit.SECONDS, FREQUENCY);
            this.frequency = frequency;
            return this;
        }

        public Builder setQueryProvider(QueryProvider queryProvider) {
            this.queryProvider = ExceptionsHelper.requireNonNull(queryProvider, QUERY.getPreferredName());
            return this;
        }

        // For testing only
        public Builder setParsedQuery(QueryBuilder queryBuilder) {
            try {
                this.queryProvider = ExceptionsHelper.requireNonNull(QueryProvider.fromParsedQuery(queryBuilder), QUERY.getPreferredName());
            } catch (IOException exception) {
                // eat exception as it should never happen
                logger.error("Exception trying to setParsedQuery", exception);
            }
            return this;
        }

        // For testing only
        public Builder setParsedAggregations(AggregatorFactories.Builder aggregations) {
            try {
                this.aggProvider = AggProvider.fromParsedAggs(aggregations);
            } catch (IOException exception) {
                // eat exception as it should never happen
                logger.error("Exception trying to setParsedAggregations", exception);
            }
            return this;
        }

        private Builder setAggregationsSafe(AggProvider provider) {
            if (this.aggProvider != null) {
                throw ExceptionsHelper.badRequestException("Found two aggregation definitions: [aggs] and [aggregations]");
            }
            this.aggProvider = provider;
            return this;
        }

        public Builder setAggProvider(AggProvider aggProvider) {
            this.aggProvider = aggProvider;
            return this;
        }

        public Builder setScriptFields(List<SearchSourceBuilder.ScriptField> scriptFields) {
            List<SearchSourceBuilder.ScriptField> sorted = new ArrayList<>(scriptFields);
            sorted.sort(Comparator.comparing(SearchSourceBuilder.ScriptField::fieldName));
            this.scriptFields = sorted;
            return this;
        }

        public Builder setScrollSize(int scrollSize) {
            if (scrollSize < 0) {
                String msg = getMessage(DATAFEED_CONFIG_INVALID_OPTION_VALUE, DatafeedConfig.SCROLL_SIZE.getPreferredName(), scrollSize);
                throw ExceptionsHelper.badRequestException(msg);
            }
            this.scrollSize = scrollSize;
            return this;
        }

        public Builder setChunkingConfig(ChunkingConfig chunkingConfig) {
            this.chunkingConfig = chunkingConfig;
            return this;
        }

        public Builder setDelayedDataCheckConfig(DelayedDataCheckConfig delayedDataCheckConfig) {
            this.delayedDataCheckConfig = delayedDataCheckConfig;
            return this;
        }

        public Builder setMaxEmptySearches(int maxEmptySearches) {
            if (maxEmptySearches == -1) {
                this.maxEmptySearches = null;
            } else if (maxEmptySearches <= 0) {
                String msg = getMessage(
                    DATAFEED_CONFIG_INVALID_OPTION_VALUE,
                    DatafeedConfig.MAX_EMPTY_SEARCHES.getPreferredName(),
                    maxEmptySearches
                );
                throw ExceptionsHelper.badRequestException(msg);
            } else {
                this.maxEmptySearches = maxEmptySearches;
            }
            return this;
        }

        public Builder setIndicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = indicesOptions;
            return this;
        }

        public IndicesOptions getIndicesOptions() {
            return this.indicesOptions;
        }

        public Builder setRuntimeMappings(Map<String, Object> runtimeMappings) {
            this.runtimeMappings = ExceptionsHelper.requireNonNull(
                runtimeMappings,
                SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD.getPreferredName()
            );
            return this;
        }

        public DatafeedConfig build() {
            ExceptionsHelper.requireNonNull(id, ID.getPreferredName());
            ExceptionsHelper.requireNonNull(jobId, JOB_ID.getPreferredName());
            if (MlStrings.isValidId(id) == false) {
                throw ExceptionsHelper.badRequestException(getMessage(INVALID_ID, ID.getPreferredName(), id));
            }
            if (indices == null || indices.isEmpty() || indices.contains("")) {
                throw invalidOptionValue(INDICES.getPreferredName(), indices);
            }

            validateScriptFields();
            RuntimeMappingsValidator.validate(runtimeMappings);
            setDefaultChunkingConfig();

            setDefaultQueryDelay();
            if (indicesOptions == null) {
                indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN_HIDDEN_FORBID_CLOSED;
            }
            return new DatafeedConfig(
                id,
                jobId,
                queryDelay,
                frequency,
                indices,
                queryProvider,
                aggProvider,
                scriptFields,
                scrollSize,
                chunkingConfig,
                headers,
                delayedDataCheckConfig,
                maxEmptySearches,
                indicesOptions,
                runtimeMappings
            );
        }

        void validateScriptFields() {
            if (aggProvider == null) {
                return;
            }
            if (scriptFields != null && scriptFields.isEmpty() == false) {
                throw ExceptionsHelper.badRequestException(getMessage(DATAFEED_CONFIG_CANNOT_USE_SCRIPT_FIELDS_WITH_AGGS));
            }
        }

        private static void checkNoMoreHistogramAggregations(Collection<AggregationBuilder> aggregations) {
            for (AggregationBuilder agg : aggregations) {
                if (DatafeedConfigUtils.isHistogram(agg)) {
                    throw ExceptionsHelper.badRequestException(DATAFEED_AGGREGATIONS_MAX_ONE_DATE_HISTOGRAM);
                }
                checkNoMoreHistogramAggregations(agg.getSubAggregations());
            }
        }

        static void checkHistogramAggregationHasChildMaxTimeAgg(AggregationBuilder histogramAggregation) {
            String timeField = null;
            if (histogramAggregation instanceof ValuesSourceAggregationBuilder) {
                timeField = ((ValuesSourceAggregationBuilder<?>) histogramAggregation).field();
            }
            if (histogramAggregation instanceof CompositeAggregationBuilder) {
                DateHistogramValuesSourceBuilder valueSource = DatafeedConfigUtils.getDateHistogramValuesSource(
                    (CompositeAggregationBuilder) histogramAggregation
                );
                timeField = valueSource.field();
            }

            for (AggregationBuilder agg : histogramAggregation.getSubAggregations()) {
                if (agg instanceof MaxAggregationBuilder maxAgg) {
                    if (maxAgg.field().equals(timeField)) {
                        return;
                    }
                }
            }

            throw ExceptionsHelper.badRequestException(getMessage(DATAFEED_DATA_HISTOGRAM_MUST_HAVE_NESTED_MAX_AGGREGATION, timeField));
        }

        private static void checkHistogramIntervalIsPositive(AggregationBuilder histogramAggregation) {
            long interval = DatafeedConfigUtils.getHistogramIntervalMillis(histogramAggregation);
            if (interval <= 0) {
                throw ExceptionsHelper.badRequestException(DATAFEED_AGGREGATIONS_INTERVAL_MUST_BE_GREATER_THAN_ZERO);
            }
        }

        static void validateCompositeAggregationSources(CompositeAggregationBuilder histogramAggregation) {
            boolean hasDateValueSource = false;
            DateHistogramValuesSourceBuilder foundBuilder = null;
            for (CompositeValuesSourceBuilder<?> valueSource : histogramAggregation.sources()) {
                if (valueSource instanceof DateHistogramValuesSourceBuilder) {
                    if (hasDateValueSource) {
                        throw ExceptionsHelper.badRequestException(
                            getMessage(DATAFEED_AGGREGATIONS_COMPOSITE_AGG_MUST_HAVE_SINGLE_DATE_SOURCE, histogramAggregation.getName())
                        );
                    }
                    hasDateValueSource = true;
                    foundBuilder = (DateHistogramValuesSourceBuilder) valueSource;
                }
            }
            if (foundBuilder == null) {
                throw ExceptionsHelper.badRequestException(
                    getMessage(DATAFEED_AGGREGATIONS_COMPOSITE_AGG_MUST_HAVE_SINGLE_DATE_SOURCE, histogramAggregation.getName())
                );
            }
            if (foundBuilder.missingBucket()) {
                throw ExceptionsHelper.badRequestException(
                    getMessage(
                        DATAFEED_AGGREGATIONS_COMPOSITE_AGG_DATE_HISTOGRAM_SOURCE_MISSING_BUCKET,
                        histogramAggregation.getName(),
                        foundBuilder.name()
                    )
                );
            }
            if (foundBuilder.order() != SortOrder.ASC) {
                throw ExceptionsHelper.badRequestException(
                    getMessage(DATAFEED_AGGREGATIONS_COMPOSITE_AGG_DATE_HISTOGRAM_SORT, histogramAggregation.getName(), foundBuilder.name())
                );
            }
        }

        private static void checkForOnlySingleTopLevelCompositeAggAndValidate(Collection<AggregationBuilder> aggregationBuilders) {
            Optional<AggregationBuilder> maybeComposite = aggregationBuilders.stream()
                .filter(agg -> agg instanceof CompositeAggregationBuilder)
                .findFirst();
            if (maybeComposite.isEmpty() == false) {
                CompositeAggregationBuilder composite = (CompositeAggregationBuilder) maybeComposite.get();
                if (aggregationBuilders.size() > 1) {
                    throw ExceptionsHelper.badRequestException(
                        getMessage(DATAFEED_AGGREGATIONS_COMPOSITE_AGG_MUST_BE_TOP_LEVEL_AND_ALONE, composite.getName())
                    );
                }
                validateCompositeAggregationSources(composite);
            }
        }

        private static void checkNoMoreCompositeAggregations(Collection<AggregationBuilder> aggregations) {
            for (AggregationBuilder agg : aggregations) {
                if (agg instanceof CompositeAggregationBuilder) {
                    throw ExceptionsHelper.badRequestException(
                        getMessage(DATAFEED_AGGREGATIONS_COMPOSITE_AGG_MUST_BE_TOP_LEVEL_AND_ALONE, agg.getName())
                    );
                }
                checkNoMoreCompositeAggregations(agg.getSubAggregations());
            }
        }

        private void setDefaultChunkingConfig() {
            if (chunkingConfig == null) {
                chunkingConfig = defaultChunkingConfig(aggProvider);
            }
        }

        private void setDefaultQueryDelay() {
            if (queryDelay == null) {
                queryDelay = defaultRandomQueryDelay(jobId);
            }
        }

        private static ElasticsearchException invalidOptionValue(String fieldName, Object value) {
            String msg = getMessage(DATAFEED_CONFIG_INVALID_OPTION_VALUE, fieldName, value);
            throw ExceptionsHelper.badRequestException(msg);
        }
    }
}
