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
 * A datafeed update contains partial properties to update a {@link DatafeedConfig}.
 * The main difference between this class and {@link DatafeedConfig} is that here all
 * fields are nullable.
 */
public class DatafeedUpdate implements ToXContentObject {

    public static final ConstructingObjectParser<Builder, Void> PARSER = new ConstructingObjectParser<>(
        "datafeed_update", true, a -> new Builder((String)a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DatafeedConfig.ID);

        PARSER.declareStringArray(Builder::setIndices, DatafeedConfig.INDEXES);
        PARSER.declareStringArray(Builder::setIndices, DatafeedConfig.INDICES);
        PARSER.declareString((builder, val) -> builder.setQueryDelay(
            TimeValue.parseTimeValue(val, DatafeedConfig.QUERY_DELAY.getPreferredName())), DatafeedConfig.QUERY_DELAY);
        PARSER.declareString((builder, val) -> builder.setFrequency(
            TimeValue.parseTimeValue(val, DatafeedConfig.FREQUENCY.getPreferredName())), DatafeedConfig.FREQUENCY);
        PARSER.declareField(Builder::setQuery, DatafeedUpdate::parseBytes, DatafeedConfig.QUERY, ObjectParser.ValueType.OBJECT);
        PARSER.declareField(Builder::setAggregations, DatafeedUpdate::parseBytes, DatafeedConfig.AGGREGATIONS,
                ObjectParser.ValueType.OBJECT);
        PARSER.declareObject(Builder::setScriptFields, (p, c) -> {
            List<SearchSourceBuilder.ScriptField> parsedScriptFields = new ArrayList<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                parsedScriptFields.add(new SearchSourceBuilder.ScriptField(p));
            }
            return parsedScriptFields;
        }, DatafeedConfig.SCRIPT_FIELDS);
        PARSER.declareInt(Builder::setScrollSize, DatafeedConfig.SCROLL_SIZE);
        PARSER.declareObject(Builder::setChunkingConfig, ChunkingConfig.PARSER, DatafeedConfig.CHUNKING_CONFIG);
        PARSER.declareObject(Builder::setDelayedDataCheckConfig,
            DelayedDataCheckConfig.PARSER,
            DatafeedConfig.DELAYED_DATA_CHECK_CONFIG);
    }

    private static BytesReference parseBytes(XContentParser parser) throws IOException {
        XContentBuilder contentBuilder = JsonXContent.contentBuilder();
        contentBuilder.generator().copyCurrentStructure(parser);
        return BytesReference.bytes(contentBuilder);
    }

    private final String id;
    private final TimeValue queryDelay;
    private final TimeValue frequency;
    private final List<String> indices;
    private final BytesReference query;
    private final BytesReference aggregations;
    private final List<SearchSourceBuilder.ScriptField> scriptFields;
    private final Integer scrollSize;
    private final ChunkingConfig chunkingConfig;
    private final DelayedDataCheckConfig delayedDataCheckConfig;

    private DatafeedUpdate(String id, TimeValue queryDelay, TimeValue frequency, List<String> indices, BytesReference query,
                           BytesReference aggregations, List<SearchSourceBuilder.ScriptField> scriptFields, Integer scrollSize,
                           ChunkingConfig chunkingConfig, DelayedDataCheckConfig delayedDataCheckConfig) {
        this.id = id;
        this.queryDelay = queryDelay;
        this.frequency = frequency;
        this.indices = indices;
        this.query = query;
        this.aggregations = aggregations;
        this.scriptFields = scriptFields;
        this.scrollSize = scrollSize;
        this.chunkingConfig = chunkingConfig;
        this.delayedDataCheckConfig = delayedDataCheckConfig;
    }

    /**
     * Get the id of the datafeed to update
     */
    public String getId() {
        return id;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DatafeedConfig.ID.getPreferredName(), id);
        if (queryDelay != null) {
            builder.field(DatafeedConfig.QUERY_DELAY.getPreferredName(), queryDelay.getStringRep());
        }
        if (frequency != null) {
            builder.field(DatafeedConfig.FREQUENCY.getPreferredName(), frequency.getStringRep());
        }
        addOptionalField(builder, DatafeedConfig.INDICES, indices);
        if (query != null) {
            builder.field(DatafeedConfig.QUERY.getPreferredName(), asMap(query));
        }
        if (aggregations != null) {
            builder.field(DatafeedConfig.AGGREGATIONS.getPreferredName(), asMap(aggregations));
        }
        if (scriptFields != null) {
            builder.startObject(DatafeedConfig.SCRIPT_FIELDS.getPreferredName());
            for (SearchSourceBuilder.ScriptField scriptField : scriptFields) {
                scriptField.toXContent(builder, params);
            }
            builder.endObject();
        }
        if (delayedDataCheckConfig != null) {
            builder.field(DatafeedConfig.DELAYED_DATA_CHECK_CONFIG.getPreferredName(), delayedDataCheckConfig);
        }
        addOptionalField(builder, DatafeedConfig.SCROLL_SIZE, scrollSize);
        addOptionalField(builder, DatafeedConfig.CHUNKING_CONFIG, chunkingConfig);
        builder.endObject();
        return builder;
    }

    private void addOptionalField(XContentBuilder builder, ParseField field, Object value) throws IOException {
        if (value != null) {
            builder.field(field.getPreferredName(), value);
        }
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

        DatafeedUpdate that = (DatafeedUpdate) other;

        return Objects.equals(this.id, that.id)
            && Objects.equals(this.frequency, that.frequency)
            && Objects.equals(this.queryDelay, that.queryDelay)
            && Objects.equals(this.indices, that.indices)
            && Objects.equals(asMap(this.query), asMap(that.query))
            && Objects.equals(this.scrollSize, that.scrollSize)
            && Objects.equals(asMap(this.aggregations), asMap(that.aggregations))
            && Objects.equals(this.delayedDataCheckConfig, that.delayedDataCheckConfig)
            && Objects.equals(this.scriptFields, that.scriptFields)
            && Objects.equals(this.chunkingConfig, that.chunkingConfig);
    }

    /**
     * Note this could be a heavy operation when a query or aggregations
     * are set as we need to convert the bytes references into maps to
     * compute a stable hash code.
     */
    @Override
    public int hashCode() {
        return Objects.hash(id, frequency, queryDelay, indices, asMap(query), scrollSize, asMap(aggregations), scriptFields,
            chunkingConfig, delayedDataCheckConfig);
    }

    public static Builder builder(String id) {
        return new Builder(id);
    }

    public static class Builder {

        private String id;
        private TimeValue queryDelay;
        private TimeValue frequency;
        private List<String> indices;
        private BytesReference query;
        private BytesReference aggregations;
        private List<SearchSourceBuilder.ScriptField> scriptFields;
        private Integer scrollSize;
        private ChunkingConfig chunkingConfig;
        private DelayedDataCheckConfig delayedDataCheckConfig;

        public Builder(String id) {
            this.id = Objects.requireNonNull(id, DatafeedConfig.ID.getPreferredName());
        }

        public Builder(DatafeedUpdate config) {
            this.id = config.id;
            this.queryDelay = config.queryDelay;
            this.frequency = config.frequency;
            this.indices = config.indices;
            this.query = config.query;
            this.aggregations = config.aggregations;
            this.scriptFields = config.scriptFields;
            this.scrollSize = config.scrollSize;
            this.chunkingConfig = config.chunkingConfig;
            this.delayedDataCheckConfig = config.delayedDataCheckConfig;
        }

        public Builder setIndices(List<String> indices) {
            this.indices = indices;
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

        public Builder setDelayedDataCheckConfig(DelayedDataCheckConfig delayedDataCheckConfig) {
            this.delayedDataCheckConfig = delayedDataCheckConfig;
            return this;
        }

        public DatafeedUpdate build() {
            return new DatafeedUpdate(id, queryDelay, frequency, indices, query, aggregations, scriptFields, scrollSize,
                chunkingConfig, delayedDataCheckConfig);
        }

        private static BytesReference xContentToBytes(ToXContentObject object) throws IOException {
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                object.toXContent(builder, ToXContentObject.EMPTY_PARAMS);
                return BytesReference.bytes(builder);
            }
        }
    }
}
