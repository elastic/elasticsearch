/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.protocol.xpack.ml.datafeed;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Datafeed configuration options pojo. Describes where to proactively pull input
 * data from.
 * <p>
 * If a value has not been set it will be <code>null</code>. Object wrappers are
 * used around integral types and booleans so they can take <code>null</code>
 * values.
 */
public class DatafeedConfig implements ToXContentObject {

    public static final int DEFAULT_SCROLL_SIZE = 1000;

    public static final ParseField ID = new ParseField("datafeed_id");
    public static final ParseField QUERY_DELAY = new ParseField("query_delay");
    public static final ParseField FREQUENCY = new ParseField("frequency");
    public static final ParseField INDEXES = new ParseField("indexes");
    public static final ParseField INDICES = new ParseField("indices");
    public static final ParseField JOB_ID = new ParseField("job_id");
    public static final ParseField TYPES = new ParseField("types");
    public static final ParseField QUERY = new ParseField("query");
    public static final ParseField SCROLL_SIZE = new ParseField("scroll_size");
    public static final ParseField AGGREGATIONS = new ParseField("aggregations");
    public static final ParseField AGGS = new ParseField("aggs");
    public static final ParseField SCRIPT_FIELDS = new ParseField("script_fields");
    public static final ParseField CHUNKING_CONFIG = new ParseField("chunking_config");

    public static final ConstructingObjectParser<Builder, Void> PARSER = new ConstructingObjectParser<>(
        "datafeed_config", true, a -> new Builder((String)a[0], (String)a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), JOB_ID);

        PARSER.declareStringArray(Builder::setIndices, INDEXES);
        PARSER.declareStringArray(Builder::setIndices, INDICES);
        PARSER.declareStringArray(Builder::setTypes, TYPES);
        PARSER.declareString((builder, val) ->
            builder.setQueryDelay(TimeValue.parseTimeValue(val, QUERY_DELAY.getPreferredName())), QUERY_DELAY);
        PARSER.declareString((builder, val) ->
            builder.setFrequency(TimeValue.parseTimeValue(val, FREQUENCY.getPreferredName())), FREQUENCY);
        PARSER.declareObject(Builder::setQuery, (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p), QUERY);
        PARSER.declareObject(Builder::setAggregations, (p, c) -> AggregatorFactories.parseAggregators(p), AGGREGATIONS);
        PARSER.declareObject(Builder::setAggregations, (p, c) -> AggregatorFactories.parseAggregators(p), AGGS);
        PARSER.declareObject(Builder::setScriptFields, (p, c) -> {
            List<SearchSourceBuilder.ScriptField> parsedScriptFields = new ArrayList<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                parsedScriptFields.add(new SearchSourceBuilder.ScriptField(p));
            }
            return parsedScriptFields;
        }, SCRIPT_FIELDS);
        PARSER.declareInt(Builder::setScrollSize, SCROLL_SIZE);
        PARSER.declareObject(Builder::setChunkingConfig, ChunkingConfig.PARSER, CHUNKING_CONFIG);
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

    private DatafeedConfig(String id, String jobId, TimeValue queryDelay, TimeValue frequency, List<String> indices, List<String> types,
                           QueryBuilder query, AggregatorFactories.Builder aggregations, List<SearchSourceBuilder.ScriptField> scriptFields,
                           Integer scrollSize, ChunkingConfig chunkingConfig) {
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

    public List<SearchSourceBuilder.ScriptField> getScriptFields() {
        return scriptFields == null ? Collections.emptyList() : scriptFields;
    }

    public ChunkingConfig getChunkingConfig() {
        return chunkingConfig;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.field(JOB_ID.getPreferredName(), jobId);
        if (queryDelay != null) {
            builder.field(QUERY_DELAY.getPreferredName(), queryDelay.getStringRep());
        }
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

        builder.endObject();
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

        if (other == null || getClass() != other.getClass()) {
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
            && Objects.equals(this.chunkingConfig, that.chunkingConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, jobId, frequency, queryDelay, indices, types, query, scrollSize, aggregations, scriptFields,
            chunkingConfig);
    }

    public static class Builder {

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

        public Builder(String id, String jobId) {
            this.id = Objects.requireNonNull(id, ID.getPreferredName());
            this.jobId = Objects.requireNonNull(jobId, JOB_ID.getPreferredName());
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
        }

        public Builder setIndices(List<String> indices) {
            this.indices = Objects.requireNonNull(indices, INDICES.getPreferredName());
            return this;
        }

        public Builder setTypes(List<String> types) {
            this.types = Objects.requireNonNull(types, TYPES.getPreferredName());
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

        public Builder setQuery(QueryBuilder query) {
            this.query = Objects.requireNonNull(query, QUERY.getPreferredName());
            return this;
        }

        public Builder setAggregations(AggregatorFactories.Builder aggregations) {
            this.aggregations = aggregations;
            return this;
        }

        public Builder setScriptFields(List<SearchSourceBuilder.ScriptField> scriptFields) {
            List<SearchSourceBuilder.ScriptField> sorted = new ArrayList<>(scriptFields);
            sorted.sort(Comparator.comparing(SearchSourceBuilder.ScriptField::fieldName));
            this.scriptFields = sorted;
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

        public DatafeedConfig build() {
            return new DatafeedConfig(id, jobId, queryDelay, frequency, indices, types, query, aggregations, scriptFields, scrollSize,
                chunkingConfig);
        }
    }
}
