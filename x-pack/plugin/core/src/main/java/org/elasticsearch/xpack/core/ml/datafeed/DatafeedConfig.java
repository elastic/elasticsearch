/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.ExtractorUtils;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.core.ml.utils.time.TimeUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Datafeed configuration options. Describes where to proactively pull input
 * data from.
 * <p>
 * If a value has not been set it will be <code>null</code>. Object wrappers are
 * used around integral types and booleans so they can take <code>null</code>
 * values.
 */
public class DatafeedConfig extends AbstractDiffable<DatafeedConfig> implements ToXContentObject {

    public static final int DEFAULT_SCROLL_SIZE = 1000;

    private static final int SECONDS_IN_MINUTE = 60;
    private static final int TWO_MINS_SECONDS = 2 * SECONDS_IN_MINUTE;
    private static final int TWENTY_MINS_SECONDS = 20 * SECONDS_IN_MINUTE;
    private static final int HALF_DAY_SECONDS = 12 * 60 * SECONDS_IN_MINUTE;

    // Used for QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("datafeeds");

    /**
     * The field name used to specify document counts in Elasticsearch
     * aggregations
     */
    public static final String DOC_COUNT = "doc_count";

    public static final ParseField ID = new ParseField("datafeed_id");
    public static final ParseField QUERY_DELAY = new ParseField("query_delay");
    public static final ParseField FREQUENCY = new ParseField("frequency");
    public static final ParseField INDEXES = new ParseField("indexes");
    public static final ParseField INDICES = new ParseField("indices");
    public static final ParseField TYPES = new ParseField("types");
    public static final ParseField QUERY = new ParseField("query");
    public static final ParseField SCROLL_SIZE = new ParseField("scroll_size");
    public static final ParseField AGGREGATIONS = new ParseField("aggregations");
    public static final ParseField AGGS = new ParseField("aggs");
    public static final ParseField SCRIPT_FIELDS = new ParseField("script_fields");
    public static final ParseField SOURCE = new ParseField("_source");
    public static final ParseField CHUNKING_CONFIG = new ParseField("chunking_config");
    public static final ParseField HEADERS = new ParseField("headers");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ObjectParser<Builder, Void> LENIENT_PARSER = createParser(true);
    public static final ObjectParser<Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, Void> parser = new ObjectParser<>("datafeed_config", ignoreUnknownFields, Builder::new);

        parser.declareString(Builder::setId, ID);
        parser.declareString(Builder::setJobId, Job.ID);
        parser.declareStringArray(Builder::setIndices, INDEXES);
        parser.declareStringArray(Builder::setIndices, INDICES);
        parser.declareStringArray(Builder::setTypes, TYPES);
        parser.declareString((builder, val) ->
            builder.setQueryDelay(TimeValue.parseTimeValue(val, QUERY_DELAY.getPreferredName())), QUERY_DELAY);
        parser.declareString((builder, val) ->
            builder.setFrequency(TimeValue.parseTimeValue(val, FREQUENCY.getPreferredName())), FREQUENCY);
        parser.declareObject(Builder::setQuery, (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p), QUERY);
        parser.declareObject(Builder::setAggregations, (p, c) -> AggregatorFactories.parseAggregators(p), AGGREGATIONS);
        parser.declareObject(Builder::setAggregations, (p, c) -> AggregatorFactories.parseAggregators(p), AGGS);
        parser.declareObject(Builder::setScriptFields, (p, c) -> {
            List<SearchSourceBuilder.ScriptField> parsedScriptFields = new ArrayList<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                parsedScriptFields.add(new SearchSourceBuilder.ScriptField(p));
            }
            parsedScriptFields.sort(Comparator.comparing(SearchSourceBuilder.ScriptField::fieldName));
            return parsedScriptFields;
        }, SCRIPT_FIELDS);
        parser.declareInt(Builder::setScrollSize, SCROLL_SIZE);
        // TODO this is to read former _source field. Remove in v7.0.0
        parser.declareBoolean((builder, value) -> {
        }, SOURCE);
        parser.declareObject(Builder::setChunkingConfig, ignoreUnknownFields ? ChunkingConfig.LENIENT_PARSER : ChunkingConfig.STRICT_PARSER,
            CHUNKING_CONFIG);

        if (ignoreUnknownFields) {
            // Headers are not parsed by the strict (config) parser, so headers supplied in the _body_ of a REST request will be rejected.
            // (For config, headers are explicitly transferred from the auth headers by code in the put/update datafeed actions.)
            parser.declareObject(Builder::setHeaders, (p, c) -> p.mapStrings(), HEADERS);
        }

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
    private final List<String> types;
    private final QueryBuilder query;
    private final AggregatorFactories.Builder aggregations;
    private final List<SearchSourceBuilder.ScriptField> scriptFields;
    private final Integer scrollSize;
    private final ChunkingConfig chunkingConfig;
    private final Map<String, String> headers;

    private DatafeedConfig(String id, String jobId, TimeValue queryDelay, TimeValue frequency, List<String> indices, List<String> types,
                           QueryBuilder query, AggregatorFactories.Builder aggregations, List<SearchSourceBuilder.ScriptField> scriptFields,
                           Integer scrollSize, ChunkingConfig chunkingConfig, Map<String, String> headers) {
        this.id = id;
        this.jobId = jobId;
        this.queryDelay = queryDelay;
        this.frequency = frequency;
        this.indices = indices == null ? null : Collections.unmodifiableList(indices);
        this.types = types == null ? null : Collections.unmodifiableList(types);
        this.query = query;
        this.aggregations = aggregations;
        this.scriptFields = scriptFields == null ? null : Collections.unmodifiableList(scriptFields);
        this.scrollSize = scrollSize;
        this.chunkingConfig = chunkingConfig;
        this.headers = Collections.unmodifiableMap(headers);
    }

    public DatafeedConfig(StreamInput in) throws IOException {
        this.id = in.readString();
        this.jobId = in.readString();
        this.queryDelay = in.readOptionalTimeValue();
        this.frequency = in.readOptionalTimeValue();
        if (in.readBoolean()) {
            this.indices = Collections.unmodifiableList(in.readList(StreamInput::readString));
        } else {
            this.indices = null;
        }
        if (in.readBoolean()) {
            this.types = Collections.unmodifiableList(in.readList(StreamInput::readString));
        } else {
            this.types = null;
        }
        this.query = in.readNamedWriteable(QueryBuilder.class);
        this.aggregations = in.readOptionalWriteable(AggregatorFactories.Builder::new);
        if (in.readBoolean()) {
            this.scriptFields = Collections.unmodifiableList(in.readList(SearchSourceBuilder.ScriptField::new));
        } else {
            this.scriptFields = null;
        }
        this.scrollSize = in.readOptionalVInt();
        this.chunkingConfig = in.readOptionalWriteable(ChunkingConfig::new);
        if (in.getVersion().onOrAfter(Version.V_6_2_0)) {
            this.headers = Collections.unmodifiableMap(in.readMap(StreamInput::readString, StreamInput::readString));
        } else {
            this.headers = Collections.emptyMap();
        }
    }

    public String getId() {
        return id;
    }

    public String getJobId() {
        return jobId;
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

    public List<String> getTypes() {
        return types;
    }

    public Integer getScrollSize() {
        return scrollSize;
    }

    public QueryBuilder getQuery() {
        return query;
    }

    public AggregatorFactories.Builder getAggregations() {
        return aggregations;
    }

    /**
     * Returns the histogram's interval as epoch millis.
     */
    public long getHistogramIntervalMillis() {
        return ExtractorUtils.getHistogramIntervalMillis(aggregations);
    }

    /**
     * @return {@code true} when there are non-empty aggregations, {@code false} otherwise
     */
    public boolean hasAggregations() {
        return aggregations != null && aggregations.count() > 0;
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(jobId);
        out.writeOptionalTimeValue(queryDelay);
        out.writeOptionalTimeValue(frequency);
        if (indices != null) {
            out.writeBoolean(true);
            out.writeStringList(indices);
        } else {
            out.writeBoolean(false);
        }
        if (types != null) {
            out.writeBoolean(true);
            out.writeStringList(types);
        } else {
            out.writeBoolean(false);
        }
        out.writeNamedWriteable(query);
        out.writeOptionalWriteable(aggregations);
        if (scriptFields != null) {
            out.writeBoolean(true);
            out.writeList(scriptFields);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalVInt(scrollSize);
        out.writeOptionalWriteable(chunkingConfig);
        if (out.getVersion().onOrAfter(Version.V_6_2_0)) {
            out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(ID.getPreferredName(), id);
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(QUERY_DELAY.getPreferredName(), queryDelay.getStringRep());
        if (frequency != null) {
            builder.field(FREQUENCY.getPreferredName(), frequency.getStringRep());
        }
        builder.field(INDICES.getPreferredName(), indices);
        builder.field(TYPES.getPreferredName(), types);
        builder.field(QUERY.getPreferredName(), query);
        if (aggregations != null) {
            builder.field(AGGREGATIONS.getPreferredName(), aggregations);
        }
        if (scriptFields != null) {
            builder.startObject(SCRIPT_FIELDS.getPreferredName());
            for (SearchSourceBuilder.ScriptField scriptField : scriptFields) {
                scriptField.toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.field(SCROLL_SIZE.getPreferredName(), scrollSize);
        if (chunkingConfig != null) {
            builder.field(CHUNKING_CONFIG.getPreferredName(), chunkingConfig);
        }
        if (headers.isEmpty() == false && params.paramAsBoolean(ToXContentParams.FOR_CLUSTER_STATE, false) == true) {
            builder.field(HEADERS.getPreferredName(), headers);
        }
        return builder;
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
                && Objects.equals(this.types, that.types)
                && Objects.equals(this.query, that.query)
                && Objects.equals(this.scrollSize, that.scrollSize)
                && Objects.equals(this.aggregations, that.aggregations)
                && Objects.equals(this.scriptFields, that.scriptFields)
                && Objects.equals(this.chunkingConfig, that.chunkingConfig)
                && Objects.equals(this.headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, jobId, frequency, queryDelay, indices, types, query, scrollSize, aggregations, scriptFields,
                chunkingConfig, headers);
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
    public TimeValue defaultFrequency(TimeValue bucketSpan) {
        TimeValue defaultFrequency = defaultFrequencyTarget(bucketSpan);
        if (hasAggregations()) {
            long histogramIntervalMillis = getHistogramIntervalMillis();
            long targetFrequencyMillis = defaultFrequency.millis();
            long defaultFrequencyMillis = histogramIntervalMillis > targetFrequencyMillis ? histogramIntervalMillis
                    : (targetFrequencyMillis / histogramIntervalMillis) * histogramIntervalMillis;
            defaultFrequency = TimeValue.timeValueMillis(defaultFrequencyMillis);
        }
        return defaultFrequency;
    }

    private TimeValue defaultFrequencyTarget(TimeValue bucketSpan) {
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

    public static class Builder {

        private static final TimeValue MIN_DEFAULT_QUERY_DELAY = TimeValue.timeValueMinutes(1);
        private static final TimeValue MAX_DEFAULT_QUERY_DELAY = TimeValue.timeValueMinutes(2);
        private static final int DEFAULT_AGGREGATION_CHUNKING_BUCKETS = 1000;

        private String id;
        private String jobId;
        private TimeValue queryDelay;
        private TimeValue frequency;
        private List<String> indices = Collections.emptyList();
        private List<String> types = Collections.emptyList();
        private QueryBuilder query = QueryBuilders.matchAllQuery();
        private AggregatorFactories.Builder aggregations;
        private List<SearchSourceBuilder.ScriptField> scriptFields;
        private Integer scrollSize = DEFAULT_SCROLL_SIZE;
        private ChunkingConfig chunkingConfig;
        private Map<String, String> headers = Collections.emptyMap();

        public Builder() {
        }

        public Builder(String id, String jobId) {
            this();
            this.id = ExceptionsHelper.requireNonNull(id, ID.getPreferredName());
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public Builder(DatafeedConfig config) {
            this.id = config.id;
            this.jobId = config.jobId;
            this.queryDelay = config.queryDelay;
            this.frequency = config.frequency;
            this.indices = config.indices;
            this.types = config.types;
            this.query = config.query;
            this.aggregations = config.aggregations;
            this.scriptFields = config.scriptFields;
            this.scrollSize = config.scrollSize;
            this.chunkingConfig = config.chunkingConfig;
            this.headers = config.headers;
        }

        public void setId(String datafeedId) {
            id = ExceptionsHelper.requireNonNull(datafeedId, ID.getPreferredName());
        }

        public void setJobId(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public void setHeaders(Map<String, String> headers) {
            this.headers = ExceptionsHelper.requireNonNull(headers, HEADERS.getPreferredName());
        }

        public void setIndices(List<String> indices) {
            this.indices = ExceptionsHelper.requireNonNull(indices, INDICES.getPreferredName());
        }

        public void setTypes(List<String> types) {
            this.types = ExceptionsHelper.requireNonNull(types, TYPES.getPreferredName());
        }

        public void setQueryDelay(TimeValue queryDelay) {
            TimeUtils.checkNonNegativeMultiple(queryDelay, TimeUnit.MILLISECONDS, QUERY_DELAY);
            this.queryDelay = queryDelay;
        }

        public void setFrequency(TimeValue frequency) {
            TimeUtils.checkPositiveMultiple(frequency, TimeUnit.SECONDS, FREQUENCY);
            this.frequency = frequency;
        }

        public void setQuery(QueryBuilder query) {
            this.query = ExceptionsHelper.requireNonNull(query, QUERY.getPreferredName());
        }

        public void setAggregations(AggregatorFactories.Builder aggregations) {
            this.aggregations = aggregations;
        }

        public void setScriptFields(List<SearchSourceBuilder.ScriptField> scriptFields) {
            List<SearchSourceBuilder.ScriptField> sorted = new ArrayList<>();
            for (SearchSourceBuilder.ScriptField scriptField : scriptFields) {
                sorted.add(scriptField);
            }
            sorted.sort(Comparator.comparing(SearchSourceBuilder.ScriptField::fieldName));
            this.scriptFields = sorted;
        }

        public void setScrollSize(int scrollSize) {
            if (scrollSize < 0) {
                String msg = Messages.getMessage(Messages.DATAFEED_CONFIG_INVALID_OPTION_VALUE,
                        DatafeedConfig.SCROLL_SIZE.getPreferredName(), scrollSize);
                throw ExceptionsHelper.badRequestException(msg);
            }
            this.scrollSize = scrollSize;
        }

        public void setChunkingConfig(ChunkingConfig chunkingConfig) {
            this.chunkingConfig = chunkingConfig;
        }

        public DatafeedConfig build() {
            ExceptionsHelper.requireNonNull(id, ID.getPreferredName());
            ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
            if (!MlStrings.isValidId(id)) {
                throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.INVALID_ID, ID.getPreferredName(), id));
            }
            if (indices == null || indices.isEmpty() || indices.contains(null) || indices.contains("")) {
                throw invalidOptionValue(INDICES.getPreferredName(), indices);
            }
            if (types == null || types.contains(null) || types.contains("")) {
                throw invalidOptionValue(TYPES.getPreferredName(), types);
            }
            validateAggregations();
            setDefaultChunkingConfig();
            setDefaultQueryDelay();
            return new DatafeedConfig(id, jobId, queryDelay, frequency, indices, types, query, aggregations, scriptFields, scrollSize,
                    chunkingConfig, headers);
        }

        void validateAggregations() {
            if (aggregations == null) {
                return;
            }
            if (scriptFields != null && !scriptFields.isEmpty()) {
                throw ExceptionsHelper.badRequestException(
                        Messages.getMessage(Messages.DATAFEED_CONFIG_CANNOT_USE_SCRIPT_FIELDS_WITH_AGGS));
            }
            List<AggregationBuilder> aggregatorFactories = aggregations.getAggregatorFactories();
            if (aggregatorFactories.isEmpty()) {
                throw ExceptionsHelper.badRequestException(Messages.DATAFEED_AGGREGATIONS_REQUIRES_DATE_HISTOGRAM);
            }

            AggregationBuilder histogramAggregation = ExtractorUtils.getHistogramAggregation(aggregatorFactories);
            checkNoMoreHistogramAggregations(histogramAggregation.getSubAggregations());
            checkHistogramAggregationHasChildMaxTimeAgg(histogramAggregation);
            checkHistogramIntervalIsPositive(histogramAggregation);
        }

        private static void checkNoMoreHistogramAggregations(List<AggregationBuilder> aggregations) {
            for (AggregationBuilder agg : aggregations) {
                if (ExtractorUtils.isHistogram(agg)) {
                    throw ExceptionsHelper.badRequestException(Messages.DATAFEED_AGGREGATIONS_MAX_ONE_DATE_HISTOGRAM);
                }
                checkNoMoreHistogramAggregations(agg.getSubAggregations());
            }
        }

        static void checkHistogramAggregationHasChildMaxTimeAgg(AggregationBuilder histogramAggregation) {
            String timeField = null;
            if (histogramAggregation instanceof ValuesSourceAggregationBuilder) {
                timeField = ((ValuesSourceAggregationBuilder) histogramAggregation).field();
            }

            for (AggregationBuilder agg : histogramAggregation.getSubAggregations()) {
                if (agg instanceof MaxAggregationBuilder) {
                    MaxAggregationBuilder maxAgg = (MaxAggregationBuilder)agg;
                    if (maxAgg.field().equals(timeField)) {
                        return;
                    }
                }
            }

            throw ExceptionsHelper.badRequestException(
                    Messages.getMessage(Messages.DATAFEED_DATA_HISTOGRAM_MUST_HAVE_NESTED_MAX_AGGREGATION, timeField));
        }

        private static void checkHistogramIntervalIsPositive(AggregationBuilder histogramAggregation) {
            long interval = ExtractorUtils.getHistogramIntervalMillis(histogramAggregation);
            if (interval <= 0) {
                throw ExceptionsHelper.badRequestException(Messages.DATAFEED_AGGREGATIONS_INTERVAL_MUST_BE_GREATER_THAN_ZERO);
            }
        }

        private void setDefaultChunkingConfig() {
            if (chunkingConfig == null) {
                if (aggregations == null) {
                    chunkingConfig = ChunkingConfig.newAuto();
                } else {
                    long histogramIntervalMillis = ExtractorUtils.getHistogramIntervalMillis(aggregations);
                    chunkingConfig = ChunkingConfig.newManual(TimeValue.timeValueMillis(
                            DEFAULT_AGGREGATION_CHUNKING_BUCKETS * histogramIntervalMillis));
                }
            }
        }

        private void setDefaultQueryDelay() {
            if (queryDelay == null) {
                Random random = new Random(jobId.hashCode());
                long delayMillis = random.longs(MIN_DEFAULT_QUERY_DELAY.millis(), MAX_DEFAULT_QUERY_DELAY.millis())
                        .findFirst().getAsLong();
                queryDelay = TimeValue.timeValueMillis(delayMillis);
            }
        }

        private static ElasticsearchException invalidOptionValue(String fieldName, Object value) {
            String msg = Messages.getMessage(Messages.DATAFEED_CONFIG_INVALID_OPTION_VALUE, fieldName, value);
            throw ExceptionsHelper.badRequestException(msg);
        }
    }
}
