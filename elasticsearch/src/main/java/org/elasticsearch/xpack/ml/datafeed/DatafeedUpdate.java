/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * A datafeed update contains partial properties to update a {@link DatafeedConfig}.
 * The main difference between this class and {@link DatafeedConfig} is that here all
 * fields are nullable.
 */
public class DatafeedUpdate implements Writeable, ToXContent {

    public static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("datafeed_update", Builder::new);

    static {
        PARSER.declareString(Builder::setId, DatafeedConfig.ID);
        PARSER.declareString(Builder::setJobId, Job.ID);
        PARSER.declareStringArray(Builder::setIndexes, DatafeedConfig.INDEXES);
        PARSER.declareStringArray(Builder::setTypes, DatafeedConfig.TYPES);
        PARSER.declareLong(Builder::setQueryDelay, DatafeedConfig.QUERY_DELAY);
        PARSER.declareLong(Builder::setFrequency, DatafeedConfig.FREQUENCY);
        PARSER.declareObject(Builder::setQuery,
                (p, c) -> new QueryParseContext(p).parseInnerQueryBuilder(), DatafeedConfig.QUERY);
        PARSER.declareObject(Builder::setAggregations, (p, c) -> AggregatorFactories.parseAggregators(new QueryParseContext(p)),
                DatafeedConfig.AGGREGATIONS);
        PARSER.declareObject(Builder::setAggregations,(p, c) -> AggregatorFactories.parseAggregators(new QueryParseContext(p)),
                DatafeedConfig.AGGS);
        PARSER.declareObject(Builder::setScriptFields, (p, c) -> {
                List<SearchSourceBuilder.ScriptField> parsedScriptFields = new ArrayList<>();
                while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                    parsedScriptFields.add(new SearchSourceBuilder.ScriptField(new QueryParseContext(p)));
            }
            parsedScriptFields.sort(Comparator.comparing(SearchSourceBuilder.ScriptField::fieldName));
            return parsedScriptFields;
        }, DatafeedConfig.SCRIPT_FIELDS);
        PARSER.declareInt(Builder::setScrollSize, DatafeedConfig.SCROLL_SIZE);
        PARSER.declareBoolean(Builder::setSource, DatafeedConfig.SOURCE);
        PARSER.declareObject(Builder::setChunkingConfig, ChunkingConfig.PARSER, DatafeedConfig.CHUNKING_CONFIG);
    }

    private final String id;
    private final String jobId;
    private final Long queryDelay;
    private final Long frequency;
    private final List<String> indexes;
    private final List<String> types;
    private final QueryBuilder query;
    private final AggregatorFactories.Builder aggregations;
    private final List<SearchSourceBuilder.ScriptField> scriptFields;
    private final Integer scrollSize;
    private final Boolean source;
    private final ChunkingConfig chunkingConfig;

    private DatafeedUpdate(String id, String jobId, Long queryDelay, Long frequency, List<String> indexes, List<String> types,
                           QueryBuilder query, AggregatorFactories.Builder aggregations, List<SearchSourceBuilder.ScriptField> scriptFields,
                           Integer scrollSize, Boolean source, ChunkingConfig chunkingConfig) {
        this.id = id;
        this.jobId = jobId;
        this.queryDelay = queryDelay;
        this.frequency = frequency;
        this.indexes = indexes;
        this.types = types;
        this.query = query;
        this.aggregations = aggregations;
        this.scriptFields = scriptFields;
        this.scrollSize = scrollSize;
        this.source = source;
        this.chunkingConfig = chunkingConfig;
    }

    public DatafeedUpdate(StreamInput in) throws IOException {
        this.id = in.readString();
        this.jobId = in.readOptionalString();
        this.queryDelay = in.readOptionalLong();
        this.frequency = in.readOptionalLong();
        if (in.readBoolean()) {
            this.indexes = in.readList(StreamInput::readString);
        } else {
            this.indexes = null;
        }
        if (in.readBoolean()) {
            this.types = in.readList(StreamInput::readString);
        } else {
            this.types = null;
        }
        this.query = in.readOptionalNamedWriteable(QueryBuilder.class);
        this.aggregations = in.readOptionalWriteable(AggregatorFactories.Builder::new);
        if (in.readBoolean()) {
            this.scriptFields = in.readList(SearchSourceBuilder.ScriptField::new);
        } else {
            this.scriptFields = null;
        }
        this.scrollSize = in.readOptionalVInt();
        this.source = in.readOptionalBoolean();
        this.chunkingConfig = in.readOptionalWriteable(ChunkingConfig::new);
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
        out.writeOptionalLong(queryDelay);
        out.writeOptionalLong(frequency);
        if (indexes != null) {
            out.writeBoolean(true);
            out.writeStringList(indexes);
        } else {
            out.writeBoolean(false);
        }
        if (types != null) {
            out.writeBoolean(true);
            out.writeStringList(types);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalNamedWriteable(query);
        out.writeOptionalWriteable(aggregations);
        if (scriptFields != null) {
            out.writeBoolean(true);
            out.writeList(scriptFields);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalVInt(scrollSize);
        out.writeOptionalBoolean(source);
        out.writeOptionalWriteable(chunkingConfig);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DatafeedConfig.ID.getPreferredName(), id);
        addOptionalField(builder, Job.ID, jobId);
        addOptionalField(builder, DatafeedConfig.QUERY_DELAY, queryDelay);
        addOptionalField(builder, DatafeedConfig.FREQUENCY, frequency);
        addOptionalField(builder, DatafeedConfig.INDEXES, indexes);
        addOptionalField(builder, DatafeedConfig.TYPES, types);
        addOptionalField(builder, DatafeedConfig.QUERY, query);
        addOptionalField(builder, DatafeedConfig.AGGREGATIONS, aggregations);
        if (scriptFields != null) {
            builder.startObject(DatafeedConfig.SCRIPT_FIELDS.getPreferredName());
            for (SearchSourceBuilder.ScriptField scriptField : scriptFields) {
                scriptField.toXContent(builder, params);
            }
            builder.endObject();
        }
        addOptionalField(builder, DatafeedConfig.SCROLL_SIZE, scrollSize);
        addOptionalField(builder, DatafeedConfig.SOURCE, source);
        addOptionalField(builder, DatafeedConfig.CHUNKING_CONFIG, chunkingConfig);
        builder.endObject();
        return builder;
    }

    private void addOptionalField(XContentBuilder builder, ParseField field, Object value) throws IOException {
        if (value != null) {
            builder.field(field.getPreferredName(), value);
        }
    }

    /**
     * Applies the update to the given {@link DatafeedConfig}
     * @return a new {@link DatafeedConfig} that contains the update
     */
    public DatafeedConfig apply(DatafeedConfig datafeedConfig) {
        if (id.equals(datafeedConfig.getId()) == false) {
            throw new IllegalArgumentException("Cannot apply update to datafeedConfig with different id");
        }

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(datafeedConfig);
        if (jobId != null) {
            builder.setJobId(jobId);
        }
        if (queryDelay != null) {
            builder.setQueryDelay(queryDelay);
        }
        if (frequency != null) {
            builder.setFrequency(frequency);
        }
        if (indexes != null) {
            builder.setIndexes(indexes);
        }
        if (types != null) {
            builder.setTypes(types);
        }
        if (query != null) {
            builder.setQuery(query);
        }
        if (aggregations != null) {
            builder.setAggregations(aggregations);
        }
        if (scriptFields != null) {
            builder.setScriptFields(scriptFields);
        }
        if (scrollSize != null) {
            builder.setScrollSize(scrollSize);
        }
        if (source != null) {
            builder.setSource(source);
        }
        if (chunkingConfig != null) {
            builder.setChunkingConfig(chunkingConfig);
        }
        return builder.build();
    }

    /**
     * The lists of indexes and types are compared for equality but they are not
     * sorted first so this test could fail simply because the indexes and types
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
                && Objects.equals(this.indexes, that.indexes)
                && Objects.equals(this.types, that.types)
                && Objects.equals(this.query, that.query)
                && Objects.equals(this.scrollSize, that.scrollSize)
                && Objects.equals(this.aggregations, that.aggregations)
                && Objects.equals(this.scriptFields, that.scriptFields)
                && Objects.equals(this.source, that.source)
                && Objects.equals(this.chunkingConfig, that.chunkingConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, jobId, frequency, queryDelay, indexes, types, query, scrollSize, aggregations, scriptFields, source,
                chunkingConfig);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class Builder {

        private String id;
        private String jobId;
        private Long queryDelay;
        private Long frequency;
        private List<String> indexes;
        private List<String> types;
        private QueryBuilder query;
        private AggregatorFactories.Builder aggregations;
        private List<SearchSourceBuilder.ScriptField> scriptFields;
        private Integer scrollSize;
        private Boolean source;
        private ChunkingConfig chunkingConfig;

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
            this.indexes = config.indexes;
            this.types = config.types;
            this.query = config.query;
            this.aggregations = config.aggregations;
            this.scriptFields = config.scriptFields;
            this.scrollSize = config.scrollSize;
            this.source = config.source;
            this.chunkingConfig = config.chunkingConfig;
        }

        public void setId(String datafeedId) {
            id = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID.getPreferredName());
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public void setIndexes(List<String> indexes) {
            this.indexes = indexes;
        }

        public void setTypes(List<String> types) {
            this.types = types;
        }

        public void setQueryDelay(long queryDelay) {
            this.queryDelay = queryDelay;
        }

        public void setFrequency(long frequency) {
            this.frequency = frequency;
        }

        public void setQuery(QueryBuilder query) {
            this.query = query;
        }

        public void setAggregations(AggregatorFactories.Builder aggregations) {
            this.aggregations = aggregations;
        }

        public void setScriptFields(List<SearchSourceBuilder.ScriptField> scriptFields) {
            List<SearchSourceBuilder.ScriptField> sorted = new ArrayList<>(scriptFields);
            sorted.sort(Comparator.comparing(SearchSourceBuilder.ScriptField::fieldName));
            this.scriptFields = sorted;
        }

        public void setScrollSize(int scrollSize) {
            this.scrollSize = scrollSize;
        }

        public void setSource(boolean enabled) {
            this.source = enabled;
        }

        public void setChunkingConfig(ChunkingConfig chunkingConfig) {
            this.chunkingConfig = chunkingConfig;
        }

        public DatafeedUpdate build() {
            return new DatafeedUpdate(id, jobId, queryDelay, frequency, indexes, types, query, aggregations, scriptFields, scrollSize,
                    source, chunkingConfig);
        }
    }
}
