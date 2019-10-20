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
package org.elasticsearch.client.ml.datafeed;

import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The datafeed configuration object. It specifies which indices
 * to get the data from and offers parameters for customizing different
 * aspects of the process.
 */
public class DatafeedConfig implements ToXContentObject {

    public static final ParseField ID = new ParseField("datafeed_id");
    public static final ParseField QUERY_DELAY = new ParseField("query_delay");
    public static final ParseField FREQUENCY = new ParseField("frequency");
    public static final ParseField INDEXES = new ParseField("indexes");
    public static final ParseField INDICES = new ParseField("indices");
    public static final ParseField QUERY = new ParseField("query");
    public static final ParseField SCROLL_SIZE = new ParseField("scroll_size");
    public static final ParseField AGGREGATIONS = new ParseField("aggregations");
    public static final ParseField SCRIPT_FIELDS = new ParseField("script_fields");
    public static final ParseField CHUNKING_CONFIG = new ParseField("chunking_config");
    public static final ParseField DELAYED_DATA_CHECK_CONFIG = new ParseField("delayed_data_check_config");
    public static final ParseField MAX_EMPTY_SEARCHES = new ParseField("max_empty_searches");

    public static final ConstructingObjectParser<Builder, Void> PARSER = new ConstructingObjectParser<>(
        "datafeed_config", true, a -> new Builder((String)a[0], (String)a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);

        PARSER.declareStringArray(Builder::setIndices, INDEXES);
        PARSER.declareStringArray(Builder::setIndices, INDICES);
        PARSER.declareString((builder, val) ->
            builder.setQueryDelay(TimeValue.parseTimeValue(val, QUERY_DELAY.getPreferredName())), QUERY_DELAY);
        PARSER.declareString((builder, val) ->
            builder.setFrequency(TimeValue.parseTimeValue(val, FREQUENCY.getPreferredName())), FREQUENCY);
        PARSER.declareField(Builder::setQuery, DatafeedConfig::parseBytes, QUERY, ObjectParser.ValueType.OBJECT);
        PARSER.declareField(Builder::setAggregations, DatafeedConfig::parseBytes, AGGREGATIONS, ObjectParser.ValueType.OBJECT);
        PARSER.declareObject(Builder::setScriptFields, (p, c) -> {
            List<SearchSourceBuilder.ScriptField> parsedScriptFields = new ArrayList<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                parsedScriptFields.add(new SearchSourceBuilder.ScriptField(p));
            }
            return parsedScriptFields;
        }, SCRIPT_FIELDS);
        PARSER.declareInt(Builder::setScrollSize, SCROLL_SIZE);
        PARSER.declareObject(Builder::setChunkingConfig, ChunkingConfig.PARSER, CHUNKING_CONFIG);
        PARSER.declareObject(Builder::setDelayedDataCheckConfig, DelayedDataCheckConfig.PARSER, DELAYED_DATA_CHECK_CONFIG);
        PARSER.declareInt(Builder::setMaxEmptySearches, MAX_EMPTY_SEARCHES);
    }

    private static BytesReference parseBytes(XContentParser parser) throws IOException {
        XContentBuilder contentBuilder = JsonXContent.contentBuilder();
        contentBuilder.generator().copyCurrentStructure(parser);
        return BytesReference.bytes(contentBuilder);
    }

    private final String id;
    private final String jobId;
    private final TimeValue queryDelay;
    private final TimeValue frequency;
    private final List<String> indices;
    private final BytesReference query;
    private final BytesReference aggregations;
    private final List<SearchSourceBuilder.ScriptField> scriptFields;
    private final Integer scrollSize;
    private final ChunkingConfig chunkingConfig;
    private final DelayedDataCheckConfig delayedDataCheckConfig;
    private final Integer maxEmptySearches;

    private DatafeedConfig(String id, String jobId, TimeValue queryDelay, TimeValue frequency, List<String> indices, BytesReference query,
                           BytesReference aggregations, List<SearchSourceBuilder.ScriptField> scriptFields, Integer scrollSize,
                           ChunkingConfig chunkingConfig, DelayedDataCheckConfig delayedDataCheckConfig,
                           Integer maxEmptySearches) {
        this.id = id;
        this.jobId = jobId;
        this.queryDelay = queryDelay;
        this.frequency = frequency;
        this.indices = indices == null ? null : Collections.unmodifiableList(indices);
        this.query = query;
        this.aggregations = aggregations;
        this.scriptFields = scriptFields == null ? null : Collections.unmodifiableList(scriptFields);
        this.scrollSize = scrollSize;
        this.chunkingConfig = chunkingConfig;
        this.delayedDataCheckConfig = delayedDataCheckConfig;
        this.maxEmptySearches = maxEmptySearches;
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

    public Integer getScrollSize() {
        return scrollSize;
    }

    public BytesReference getQuery() {
        return query;
    }

    public BytesReference getAggregations() {
        return aggregations;
    }

    public List<SearchSourceBuilder.ScriptField> getScriptFields() {
        return scriptFields == null ? Collections.emptyList() : scriptFields;
    }

    public ChunkingConfig getChunkingConfig() {
        return chunkingConfig;
    }

    public DelayedDataCheckConfig getDelayedDataCheckConfig() {
        return delayedDataCheckConfig;
    }

    public Integer getMaxEmptySearches() {
        return maxEmptySearches;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.field(Job.ID.getPreferredName(), jobId);
        if (queryDelay != null) {
            builder.field(QUERY_DELAY.getPreferredName(), queryDelay.getStringRep());
        }
        if (frequency != null) {
            builder.field(FREQUENCY.getPreferredName(), frequency.getStringRep());
        }
        if (indices != null) {
            builder.field(INDICES.getPreferredName(), indices);
        }
        if (query != null) {
            builder.field(QUERY.getPreferredName(), asMap(query));
        }
        if (aggregations != null) {
            builder.field(AGGREGATIONS.getPreferredName(), asMap(aggregations));
        }
        if (scriptFields != null) {
            builder.startObject(SCRIPT_FIELDS.getPreferredName());
            for (SearchSourceBuilder.ScriptField scriptField : scriptFields) {
                scriptField.toXContent(builder, params);
            }
            builder.endObject();
        }
        if (scrollSize != null) {
            builder.field(SCROLL_SIZE.getPreferredName(), scrollSize);
        }
        if (chunkingConfig != null) {
            builder.field(CHUNKING_CONFIG.getPreferredName(), chunkingConfig);
        }
        if (delayedDataCheckConfig != null) {
            builder.field(DELAYED_DATA_CHECK_CONFIG.getPreferredName(), delayedDataCheckConfig);
        }
        if (maxEmptySearches != null) {
            builder.field(MAX_EMPTY_SEARCHES.getPreferredName(), maxEmptySearches);
        }

        builder.endObject();
        return builder;
    }

    private static Map<String, Object> asMap(BytesReference bytesReference) {
        return bytesReference == null ? null : XContentHelper.convertToMap(bytesReference, true, XContentType.JSON).v2();
    }

    /**
     * The lists of indices and types are compared for equality but they are not
     * sorted first so this test could fail simply because the indices and types
     * lists are in different orders.
     *
     * Also note this could be a heavy operation when a query or aggregations
     * are set as we need to convert the bytes references into maps to correctly
     * compare them.
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
            && Objects.equals(asMap(this.query), asMap(that.query))
            && Objects.equals(this.scrollSize, that.scrollSize)
            && Objects.equals(asMap(this.aggregations), asMap(that.aggregations))
            && Objects.equals(this.scriptFields, that.scriptFields)
            && Objects.equals(this.chunkingConfig, that.chunkingConfig)
            && Objects.equals(this.delayedDataCheckConfig, that.delayedDataCheckConfig)
            && Objects.equals(this.maxEmptySearches, that.maxEmptySearches);
    }

    /**
     * Note this could be a heavy operation when a query or aggregations
     * are set as we need to convert the bytes references into maps to
     * compute a stable hash code.
     */
    @Override
    public int hashCode() {
        return Objects.hash(id, jobId, frequency, queryDelay, indices, asMap(query), scrollSize, asMap(aggregations), scriptFields,
            chunkingConfig, delayedDataCheckConfig, maxEmptySearches);
    }

    public static Builder builder(String id, String jobId) {
        return new Builder(id, jobId);
    }

    public static class Builder {

        private String id;
        private String jobId;
        private TimeValue queryDelay;
        private TimeValue frequency;
        private List<String> indices;
        private BytesReference query;
        private BytesReference aggregations;
        private List<SearchSourceBuilder.ScriptField> scriptFields;
        private Integer scrollSize;
        private ChunkingConfig chunkingConfig;
        private DelayedDataCheckConfig delayedDataCheckConfig;
        private Integer maxEmptySearches;

        public Builder(String id, String jobId) {
            this.id = Objects.requireNonNull(id, ID.getPreferredName());
            this.jobId = Objects.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public Builder(DatafeedConfig config) {
            this.id = config.id;
            this.jobId = config.jobId;
            this.queryDelay = config.queryDelay;
            this.frequency = config.frequency;
            this.indices = config.indices == null ? null : new ArrayList<>(config.indices);
            this.query = config.query;
            this.aggregations = config.aggregations;
            this.scriptFields = config.scriptFields == null ? null : new ArrayList<>(config.scriptFields);
            this.scrollSize = config.scrollSize;
            this.chunkingConfig = config.chunkingConfig;
            this.delayedDataCheckConfig = config.getDelayedDataCheckConfig();
            this.maxEmptySearches = config.getMaxEmptySearches();
        }

        public Builder setIndices(List<String> indices) {
            this.indices = Objects.requireNonNull(indices, INDICES.getPreferredName());
            return this;
        }

        public Builder setIndices(String... indices) {
            return setIndices(Arrays.asList(indices));
        }

        public Builder setQueryDelay(TimeValue queryDelay) {
            this.queryDelay = queryDelay;
            return this;
        }

        public Builder setFrequency(TimeValue frequency) {
            this.frequency = frequency;
            return this;
        }

        private Builder setQuery(BytesReference query) {
            this.query = query;
            return this;
        }

        public Builder setQuery(String queryAsJson) {
            this.query = queryAsJson == null ? null : new BytesArray(queryAsJson);
            return this;
        }

        public Builder setQuery(QueryBuilder query) throws IOException {
            this.query = query == null ? null : xContentToBytes(query);
            return this;
        }

        private Builder setAggregations(BytesReference aggregations) {
            this.aggregations = aggregations;
            return this;
        }

        public Builder setAggregations(String aggsAsJson) {
            this.aggregations = aggsAsJson == null ? null : new BytesArray(aggsAsJson);
            return this;
        }

        public Builder setAggregations(AggregatorFactories.Builder aggregations) throws IOException {
            this.aggregations = aggregations == null ? null : xContentToBytes(aggregations);
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

        /**
         * This sets the {@link DelayedDataCheckConfig} settings.
         *
         * See {@link DelayedDataCheckConfig} for more information.
         *
         * @param delayedDataCheckConfig the delayed data check configuration
         *                               Default value is enabled, with `check_window` being null. This means the true window is
         *                               calculated when the real-time Datafeed runs.
         */
        public Builder setDelayedDataCheckConfig(DelayedDataCheckConfig delayedDataCheckConfig) {
            this.delayedDataCheckConfig = delayedDataCheckConfig;
            return this;
        }

        public Builder setMaxEmptySearches(int maxEmptySearches) {
            this.maxEmptySearches = maxEmptySearches;
            return this;
        }

        public DatafeedConfig build() {
            return new DatafeedConfig(id, jobId, queryDelay, frequency, indices, query, aggregations, scriptFields, scrollSize,
                chunkingConfig, delayedDataCheckConfig, maxEmptySearches);
        }

        private static BytesReference xContentToBytes(ToXContentObject object) throws IOException {
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                object.toXContent(builder, ToXContentObject.EMPTY_PARAMS);
                return BytesReference.bytes(builder);
            }
        }
    }
}
