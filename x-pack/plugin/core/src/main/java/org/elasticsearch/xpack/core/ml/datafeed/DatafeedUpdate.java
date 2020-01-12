/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;
import org.elasticsearch.xpack.core.ml.utils.XContentObjectTransformer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


/**
 * A datafeed update contains partial properties to update a {@link DatafeedConfig}.
 * The main difference between this class and {@link DatafeedConfig} is that here all
 * fields are nullable.
 */
public class DatafeedUpdate implements Writeable, ToXContentObject {

    static final String ERROR_MESSAGE_ON_JOB_ID_UPDATE = "Datafeed's job_id cannot be changed.";

    public static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("datafeed_update", Builder::new);

    static {
        PARSER.declareString(Builder::setId, DatafeedConfig.ID);
        PARSER.declareString(Builder::setJobId, Job.ID);
        PARSER.declareStringArray(Builder::setIndices, DatafeedConfig.INDEXES);
        PARSER.declareStringArray(Builder::setIndices, DatafeedConfig.INDICES);
        PARSER.declareString((builder, val) -> builder.setQueryDelay(
                TimeValue.parseTimeValue(val, DatafeedConfig.QUERY_DELAY.getPreferredName())), DatafeedConfig.QUERY_DELAY);
        PARSER.declareString((builder, val) -> builder.setFrequency(
                TimeValue.parseTimeValue(val, DatafeedConfig.FREQUENCY.getPreferredName())), DatafeedConfig.FREQUENCY);
        PARSER.declareObject(Builder::setQuery, (p, c) -> QueryProvider.fromXContent(p, false, Messages.DATAFEED_CONFIG_QUERY_BAD_FORMAT),
            DatafeedConfig.QUERY);
        PARSER.declareObject(Builder::setAggregationsSafe,
            (p, c) -> AggProvider.fromXContent(p, false),
            DatafeedConfig.AGGREGATIONS);
        PARSER.declareObject(Builder::setAggregationsSafe,
            (p, c) -> AggProvider.fromXContent(p, false),
            DatafeedConfig.AGGS);
        PARSER.declareObject(Builder::setScriptFields, (p, c) -> {
                List<SearchSourceBuilder.ScriptField> parsedScriptFields = new ArrayList<>();
                while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                    parsedScriptFields.add(new SearchSourceBuilder.ScriptField(p));
            }
            parsedScriptFields.sort(Comparator.comparing(SearchSourceBuilder.ScriptField::fieldName));
            return parsedScriptFields;
        }, DatafeedConfig.SCRIPT_FIELDS);
        PARSER.declareInt(Builder::setScrollSize, DatafeedConfig.SCROLL_SIZE);
        PARSER.declareObject(Builder::setChunkingConfig, ChunkingConfig.STRICT_PARSER, DatafeedConfig.CHUNKING_CONFIG);
        PARSER.declareObject(Builder::setDelayedDataCheckConfig,
            DelayedDataCheckConfig.STRICT_PARSER,
            DatafeedConfig.DELAYED_DATA_CHECK_CONFIG);
        PARSER.declareInt(Builder::setMaxEmptySearches, DatafeedConfig.MAX_EMPTY_SEARCHES);
    }

    private final String id;
    private final String jobId;
    private final TimeValue queryDelay;
    private final TimeValue frequency;
    private final List<String> indices;
    private final QueryProvider queryProvider;
    private final AggProvider aggProvider;
    private final List<SearchSourceBuilder.ScriptField> scriptFields;
    private final Integer scrollSize;
    private final ChunkingConfig chunkingConfig;
    private final DelayedDataCheckConfig delayedDataCheckConfig;
    private final Integer maxEmptySearches;

    private DatafeedUpdate(String id, String jobId, TimeValue queryDelay, TimeValue frequency, List<String> indices,
                           QueryProvider queryProvider, AggProvider aggProvider,
                           List<SearchSourceBuilder.ScriptField> scriptFields,
                           Integer scrollSize, ChunkingConfig chunkingConfig, DelayedDataCheckConfig delayedDataCheckConfig,
                           Integer maxEmptySearches) {
        this.id = id;
        this.jobId = jobId;
        this.queryDelay = queryDelay;
        this.frequency = frequency;
        this.indices = indices;
        this.queryProvider = queryProvider;
        this.aggProvider = aggProvider;
        this.scriptFields = scriptFields;
        this.scrollSize = scrollSize;
        this.chunkingConfig = chunkingConfig;
        this.delayedDataCheckConfig = delayedDataCheckConfig;
        this.maxEmptySearches = maxEmptySearches;
    }

    public DatafeedUpdate(StreamInput in) throws IOException {
        this.id = in.readString();
        this.jobId = in.readOptionalString();
        this.queryDelay = in.readOptionalTimeValue();
        this.frequency = in.readOptionalTimeValue();
        if (in.readBoolean()) {
            this.indices = in.readStringList();
        } else {
            this.indices = null;
        }
        // This consumes the list of types if there was one.
        if (in.getVersion().before(Version.V_7_0_0)) {
            if (in.readBoolean()) {
                in.readStringList();
            }
        }
        if (in.getVersion().before(Version.V_7_0_0)) {
            this.queryProvider = QueryProvider.fromParsedQuery(in.readOptionalNamedWriteable(QueryBuilder.class));
            this.aggProvider = AggProvider.fromParsedAggs(in.readOptionalWriteable(AggregatorFactories.Builder::new));
        } else {
            this.queryProvider = in.readOptionalWriteable(QueryProvider::fromStream);
            this.aggProvider = in.readOptionalWriteable(AggProvider::fromStream);
        }
        if (in.readBoolean()) {
            this.scriptFields = in.readList(SearchSourceBuilder.ScriptField::new);
        } else {
            this.scriptFields = null;
        }
        this.scrollSize = in.readOptionalVInt();
        this.chunkingConfig = in.readOptionalWriteable(ChunkingConfig::new);
        delayedDataCheckConfig = in.readOptionalWriteable(DelayedDataCheckConfig::new);
        if (in.getVersion().onOrAfter(Version.V_7_5_0)) {
            maxEmptySearches = in.readOptionalInt();
        } else {
            maxEmptySearches = null;
        }
    }

    /**
     * Get the id of the datafeed to update
     */
    public String getId() {
        return id;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeOptionalString(jobId);
        out.writeOptionalTimeValue(queryDelay);
        out.writeOptionalTimeValue(frequency);
        if (indices != null) {
            out.writeBoolean(true);
            out.writeStringCollection(indices);
        } else {
            out.writeBoolean(false);
        }
        // Write the now removed types to prior versions.
        // An empty list is expected
        if (out.getVersion().before(Version.V_7_0_0)) {
            out.writeBoolean(true);
            out.writeStringCollection(Collections.emptyList());
        }
        if (out.getVersion().before(Version.V_7_0_0)) {
            out.writeOptionalNamedWriteable(queryProvider == null ? null : queryProvider.getParsedQuery());
            out.writeOptionalWriteable(aggProvider == null ? null : aggProvider.getParsedAggs());
        } else {
            out.writeOptionalWriteable(queryProvider);
            out.writeOptionalWriteable(aggProvider);
        }
        if (scriptFields != null) {
            out.writeBoolean(true);
            out.writeList(scriptFields);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalVInt(scrollSize);
        out.writeOptionalWriteable(chunkingConfig);
        out.writeOptionalWriteable(delayedDataCheckConfig);
        if (out.getVersion().onOrAfter(Version.V_7_5_0)) {
            out.writeOptionalInt(maxEmptySearches);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DatafeedConfig.ID.getPreferredName(), id);
        addOptionalField(builder, Job.ID, jobId);
        if (queryDelay != null) {
            builder.field(DatafeedConfig.QUERY_DELAY.getPreferredName(), queryDelay.getStringRep());
        }
        if (frequency != null) {
            builder.field(DatafeedConfig.FREQUENCY.getPreferredName(), frequency.getStringRep());
        }
        addOptionalField(builder, DatafeedConfig.INDICES, indices);
        if (queryProvider != null) {
            builder.field(DatafeedConfig.QUERY.getPreferredName(), queryProvider.getQuery());
        }
        if (aggProvider != null) {
            builder.field(DatafeedConfig.AGGREGATIONS.getPreferredName(), aggProvider.getAggs());
        }
        if (scriptFields != null) {
            builder.startObject(DatafeedConfig.SCRIPT_FIELDS.getPreferredName());
            for (SearchSourceBuilder.ScriptField scriptField : scriptFields) {
                scriptField.toXContent(builder, params);
            }
            builder.endObject();
        }
        addOptionalField(builder, DatafeedConfig.SCROLL_SIZE, scrollSize);
        addOptionalField(builder, DatafeedConfig.CHUNKING_CONFIG, chunkingConfig);
        addOptionalField(builder, DatafeedConfig.DELAYED_DATA_CHECK_CONFIG, delayedDataCheckConfig);
        addOptionalField(builder, DatafeedConfig.MAX_EMPTY_SEARCHES, maxEmptySearches);

        builder.endObject();
        return builder;
    }

    private void addOptionalField(XContentBuilder builder, ParseField field, Object value) throws IOException {
        if (value != null) {
            builder.field(field.getPreferredName(), value);
        }
    }

    public String getJobId() {
        return jobId;
    }

    TimeValue getQueryDelay() {
        return queryDelay;
    }

    TimeValue getFrequency() {
        return frequency;
    }

    List<String> getIndices() {
        return indices;
    }

    Integer getScrollSize() {
        return scrollSize;
    }

    Map<String, Object> getQuery() {
        return queryProvider == null ? null : queryProvider.getQuery();
    }

    QueryBuilder getParsedQuery(NamedXContentRegistry namedXContentRegistry) throws IOException {
        return XContentObjectTransformer.queryBuilderTransformer(namedXContentRegistry).fromMap(queryProvider.getQuery(),
                new ArrayList<>());
    }

    Map<String, Object> getAggregations() {
        return aggProvider == null ? null : aggProvider.getAggs();
    }

    AggregatorFactories.Builder getParsedAgg(NamedXContentRegistry namedXContentRegistry) throws IOException {
        return XContentObjectTransformer.aggregatorTransformer(namedXContentRegistry).fromMap(aggProvider.getAggs(),
            new ArrayList<>());
    }

    /**
     * @return {@code true} when there are non-empty aggregations, {@code false}
     *         otherwise
     */
    boolean hasAggregations() {
        return getAggregations() != null && getAggregations().size() > 0;
    }

    List<SearchSourceBuilder.ScriptField> getScriptFields() {
        return scriptFields == null ? Collections.emptyList() : scriptFields;
    }

    ChunkingConfig getChunkingConfig() {
        return chunkingConfig;
    }

    public DelayedDataCheckConfig getDelayedDataCheckConfig() {
        return delayedDataCheckConfig;
    }

    public Integer getMaxEmptySearches() {
        return maxEmptySearches;
    }

    /**
     * Applies the update to the given {@link DatafeedConfig}
     * @return a new {@link DatafeedConfig} that contains the update
     */
    public DatafeedConfig apply(DatafeedConfig datafeedConfig, Map<String, String> headers) {
        if (id.equals(datafeedConfig.getId()) == false) {
            throw new IllegalArgumentException("Cannot apply update to datafeedConfig with different id");
        }

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(datafeedConfig);
        if (jobId != null) {
            if (datafeedConfig.getJobId() != null && datafeedConfig.getJobId().equals(jobId) == false) {
                throw ExceptionsHelper.badRequestException(ERROR_MESSAGE_ON_JOB_ID_UPDATE);
            }
            builder.setJobId(jobId);
        }
        if (queryDelay != null) {
            builder.setQueryDelay(queryDelay);
        }
        if (frequency != null) {
            builder.setFrequency(frequency);
        }
        if (indices != null) {
            builder.setIndices(indices);
        }
        if (queryProvider != null) {
            builder.setQueryProvider(queryProvider);
        }
        if (aggProvider != null) {
            DatafeedConfig.validateAggregations(aggProvider.getParsedAggs());
            builder.setAggProvider(aggProvider);
        }
        if (scriptFields != null) {
            builder.setScriptFields(scriptFields);
        }
        if (scrollSize != null) {
            builder.setScrollSize(scrollSize);
        }
        if (chunkingConfig != null) {
            builder.setChunkingConfig(chunkingConfig);
        }
        if (delayedDataCheckConfig != null) {
            builder.setDelayedDataCheckConfig(delayedDataCheckConfig);
        }
        if (maxEmptySearches != null) {
            builder.setMaxEmptySearches(maxEmptySearches);
        }

        if (headers.isEmpty() == false) {
            // Adjust the request, adding security headers from the current thread context
            Map<String, String> securityHeaders = headers.entrySet().stream()
                    .filter(e -> ClientHelper.SECURITY_HEADER_FILTERS.contains(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            builder.setHeaders(securityHeaders);
        }

        return builder.build();
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

        if (other instanceof DatafeedUpdate == false) {
            return false;
        }

        DatafeedUpdate that = (DatafeedUpdate) other;

        return Objects.equals(this.id, that.id)
                && Objects.equals(this.jobId, that.jobId)
                && Objects.equals(this.frequency, that.frequency)
                && Objects.equals(this.queryDelay, that.queryDelay)
                && Objects.equals(this.indices, that.indices)
                && Objects.equals(this.queryProvider, that.queryProvider)
                && Objects.equals(this.scrollSize, that.scrollSize)
                && Objects.equals(this.aggProvider, that.aggProvider)
                && Objects.equals(this.delayedDataCheckConfig, that.delayedDataCheckConfig)
                && Objects.equals(this.scriptFields, that.scriptFields)
                && Objects.equals(this.chunkingConfig, that.chunkingConfig)
                && Objects.equals(this.maxEmptySearches, that.maxEmptySearches);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, jobId, frequency, queryDelay, indices, queryProvider, scrollSize, aggProvider, scriptFields, chunkingConfig,
                delayedDataCheckConfig, maxEmptySearches);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    boolean isNoop(DatafeedConfig datafeed) {
        return (frequency == null || Objects.equals(frequency, datafeed.getFrequency()))
                && (queryDelay == null || Objects.equals(queryDelay, datafeed.getQueryDelay()))
                && (indices == null || Objects.equals(indices, datafeed.getIndices()))
                && (queryProvider == null || Objects.equals(queryProvider.getQuery(), datafeed.getQuery()))
                && (scrollSize == null || Objects.equals(scrollSize, datafeed.getScrollSize()))
                && (aggProvider == null || Objects.equals(aggProvider.getAggs(), datafeed.getAggregations()))
                && (scriptFields == null || Objects.equals(scriptFields, datafeed.getScriptFields()))
                && (delayedDataCheckConfig == null || Objects.equals(delayedDataCheckConfig, datafeed.getDelayedDataCheckConfig()))
                && (chunkingConfig == null || Objects.equals(chunkingConfig, datafeed.getChunkingConfig()))
                && (maxEmptySearches == null || Objects.equals(maxEmptySearches, datafeed.getMaxEmptySearches())
                        || (maxEmptySearches == -1 && datafeed.getMaxEmptySearches() == null));
    }

    public static class Builder {

        private String id;
        private String jobId;
        private TimeValue queryDelay;
        private TimeValue frequency;
        private List<String> indices;
        private QueryProvider queryProvider;
        private AggProvider aggProvider;
        private List<SearchSourceBuilder.ScriptField> scriptFields;
        private Integer scrollSize;
        private ChunkingConfig chunkingConfig;
        private DelayedDataCheckConfig delayedDataCheckConfig;
        private Integer maxEmptySearches;

        public Builder() {
        }

        public Builder(String id) {
            this.id = ExceptionsHelper.requireNonNull(id, DatafeedConfig.ID.getPreferredName());
        }

        public Builder(DatafeedUpdate config) {
            this.id = config.id;
            this.jobId = config.jobId;
            this.queryDelay = config.queryDelay;
            this.frequency = config.frequency;
            this.indices = config.indices;
            this.queryProvider = config.queryProvider;
            this.aggProvider = config.aggProvider;
            this.scriptFields = config.scriptFields;
            this.scrollSize = config.scrollSize;
            this.chunkingConfig = config.chunkingConfig;
            this.delayedDataCheckConfig = config.delayedDataCheckConfig;
            this.maxEmptySearches = config.maxEmptySearches;
        }

        public Builder setId(String datafeedId) {
            id = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID.getPreferredName());
            return this;
        }

        public Builder setJobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder setIndices(List<String> indices) {
            this.indices = indices;
            return this;
        }

        public Builder setQueryDelay(TimeValue queryDelay) {
            this.queryDelay = queryDelay;
            return this;
        }

        public Builder setFrequency(TimeValue frequency) {
            this.frequency = frequency;
            return this;
        }

        public Builder setQuery(QueryProvider queryProvider) {
            this.queryProvider = queryProvider;
            return this;
        }

        private Builder setAggregationsSafe(AggProvider aggProvider) {
            if (this.aggProvider != null) {
                throw ExceptionsHelper.badRequestException("Found two aggregation definitions: [aggs] and [aggregations]");
            }
            setAggregations(aggProvider);
            return this;
        }

        public Builder setAggregations(AggProvider aggProvider) {
            this.aggProvider = aggProvider;
            return this;
        }

        public Builder setScriptFields(List<SearchSourceBuilder.ScriptField> scriptFields) {
            List<SearchSourceBuilder.ScriptField> sorted = new ArrayList<>(scriptFields);
            sorted.sort(Comparator.comparing(SearchSourceBuilder.ScriptField::fieldName));
            this.scriptFields = sorted;
            return this;
        }

        public Builder setDelayedDataCheckConfig(DelayedDataCheckConfig delayedDataCheckConfig) {
            this.delayedDataCheckConfig = delayedDataCheckConfig;
            return this;
        }

        public Builder setScrollSize(int scrollSize) {
            this.scrollSize = scrollSize;
            return this;
        }

        public Builder setChunkingConfig(ChunkingConfig chunkingConfig) {
            this.chunkingConfig = chunkingConfig;
            return this;
        }

        public Builder setMaxEmptySearches(int maxEmptySearches) {
            if (maxEmptySearches < -1 || maxEmptySearches == 0) {
                String msg = Messages.getMessage(Messages.DATAFEED_CONFIG_INVALID_OPTION_VALUE,
                    DatafeedConfig.MAX_EMPTY_SEARCHES.getPreferredName(), maxEmptySearches);
                throw ExceptionsHelper.badRequestException(msg);
            }
            this.maxEmptySearches = maxEmptySearches;
            return this;
        }

        public DatafeedUpdate build() {
            return new DatafeedUpdate(id, jobId, queryDelay, frequency, indices, queryProvider, aggProvider, scriptFields, scrollSize,
                    chunkingConfig, delayedDataCheckConfig, maxEmptySearches);
        }
    }
}
