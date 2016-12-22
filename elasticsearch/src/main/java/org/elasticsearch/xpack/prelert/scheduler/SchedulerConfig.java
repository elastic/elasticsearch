/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.scheduler;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.messages.Messages;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;
import org.elasticsearch.xpack.prelert.utils.PrelertStrings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Scheduler configuration options. Describes where to proactively pull input
 * data from.
 * <p>
 * If a value has not been set it will be <code>null</code>. Object wrappers are
 * used around integral types and booleans so they can take <code>null</code>
 * values.
 */
public class SchedulerConfig extends ToXContentToBytes implements Writeable {

    /**
     * The field name used to specify aggregation fields in Elasticsearch
     * aggregations
     */
    private static final String FIELD = "field";
    /**
     * The field name used to specify document counts in Elasticsearch
     * aggregations
     */
    public static final String DOC_COUNT = "doc_count";

    public static final ParseField ID = new ParseField("scheduler_id");
    public static final ParseField QUERY_DELAY = new ParseField("query_delay");
    public static final ParseField FREQUENCY = new ParseField("frequency");
    public static final ParseField INDEXES = new ParseField("indexes");
    public static final ParseField TYPES = new ParseField("types");
    public static final ParseField QUERY = new ParseField("query");
    public static final ParseField SCROLL_SIZE = new ParseField("scroll_size");
    public static final ParseField AGGREGATIONS = new ParseField("aggregations");
    public static final ParseField AGGS = new ParseField("aggs");
    public static final ParseField SCRIPT_FIELDS = new ParseField("script_fields");

    public static final ObjectParser<Builder, ParseFieldMatcherSupplier> PARSER = new ObjectParser<>("scheduler_config", Builder::new);

    static {
        PARSER.declareString(Builder::setId, ID);
        PARSER.declareString(Builder::setJobId, Job.ID);
        PARSER.declareStringArray(Builder::setIndexes, INDEXES);
        PARSER.declareStringArray(Builder::setTypes, TYPES);
        PARSER.declareLong(Builder::setQueryDelay, QUERY_DELAY);
        PARSER.declareLong(Builder::setFrequency, FREQUENCY);
        PARSER.declareField((parser, builder, aVoid) -> {
            XContentBuilder contentBuilder = XContentBuilder.builder(parser.contentType().xContent());
            XContentHelper.copyCurrentStructure(contentBuilder.generator(), parser);
            builder.setQuery(contentBuilder.bytes());
        }, QUERY, ObjectParser.ValueType.OBJECT);
        PARSER.declareField((parser, builder, aVoid) -> {
            XContentBuilder contentBuilder = XContentBuilder.builder(parser.contentType().xContent());
            XContentHelper.copyCurrentStructure(contentBuilder.generator(), parser);
            builder.setAggregations(contentBuilder.bytes());
        }, AGGREGATIONS, ObjectParser.ValueType.OBJECT);
        PARSER.declareField((parser, builder, aVoid) -> {
            XContentBuilder contentBuilder = XContentBuilder.builder(parser.contentType().xContent());
            XContentHelper.copyCurrentStructure(contentBuilder.generator(), parser);
            builder.setAggregations(contentBuilder.bytes());
        }, AGGS, ObjectParser.ValueType.OBJECT);
        PARSER.declareField((parser, builder, aVoid) -> {
            XContentBuilder contentBuilder = XContentBuilder.builder(parser.contentType().xContent());
            XContentHelper.copyCurrentStructure(contentBuilder.generator(), parser);
            builder.setScriptFields(contentBuilder.bytes());
        }, SCRIPT_FIELDS, ObjectParser.ValueType.OBJECT);
        PARSER.declareInt(Builder::setScrollSize, SCROLL_SIZE);
    }

    private final String id;
    private final String jobId;

    /**
     * The delay in seconds before starting to query a period of time
     */
    private final Long queryDelay;

    /**
     * The frequency in seconds with which queries are executed
     */
    private final Long frequency;

    private final List<String> indexes;
    private final List<String> types;
    private final BytesReference query;
    private final BytesReference aggregations;
    private final BytesReference scriptFields;
    private final Integer scrollSize;

    private SchedulerConfig(String id, String jobId, Long queryDelay, Long frequency, List<String> indexes, List<String> types,
                            BytesReference query, BytesReference aggregations, BytesReference scriptFields, Integer scrollSize) {
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
    }

    public SchedulerConfig(StreamInput in) throws IOException {
        this.id = in.readString();
        this.jobId = in.readString();
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
        this.query = in.readOptionalBytesReference();
        this.aggregations = in.readOptionalBytesReference();
        this.scriptFields = in.readOptionalBytesReference();
        this.scrollSize = in.readOptionalVInt();
    }

    public String getId() {
        return id;
    }

    public String getJobId() {
        return jobId;
    }

    public Long getQueryDelay() {
        return queryDelay;
    }

    public Long getFrequency() {
        return frequency;
    }

    /**
     * For the ELASTICSEARCH data source only, one or more indexes to search for
     * input data.
     *
     * @return The indexes to search, or <code>null</code> if not set.
     */
    public List<String> getIndexes() {
        return this.indexes;
    }

    /**
     * For the ELASTICSEARCH data source only, one or more types to search for
     * input data.
     *
     * @return The types to search, or <code>null</code> if not set.
     */
    public List<String> getTypes() {
        return this.types;
    }

    public Integer getScrollSize() {
        return this.scrollSize;
    }

    Map<String, Object> getQueryAsMap() {
        return XContentHelper.convertToMap(query, true).v2();
    }

    Map<String, Object> getAggregationsAsMap() {
        return XContentHelper.convertToMap(aggregations, true).v2();
    }

    Map<String, Object> getScriptFieldsAsMap() {
        return XContentHelper.convertToMap(scriptFields, true).v2();
    }

    public QueryBuilder buildQuery(IndicesQueriesRegistry queryParsers) {
        if (query == null) {
            return QueryBuilders.matchAllQuery();
        }
        XContentParser parser = createParser(QUERY, query);
        QueryParseContext queryParseContext = new QueryParseContext(queryParsers, parser, ParseFieldMatcher.STRICT);
        try {
            return queryParseContext.parseInnerQueryBuilder().orElse(QueryBuilders.matchAllQuery());
        } catch (IOException e) {
            throw ExceptionsHelper.parseException(QUERY, e);
        }
    }

    public boolean hasAggregations() {
        return aggregations != null;
    }

    public AggregatorFactories.Builder buildAggregations(IndicesQueriesRegistry queryParsers, AggregatorParsers aggParsers) {
        if (!hasAggregations()) {
            return null;
        }
        XContentParser parser = createParser(AGGREGATIONS, aggregations);
        QueryParseContext queryParseContext = new QueryParseContext(queryParsers, parser, ParseFieldMatcher.STRICT);
        try {
            return aggParsers.parseAggregators(queryParseContext);
        } catch (IOException e) {
            throw ExceptionsHelper.parseException(AGGREGATIONS, e);
        }
    }

    public List<SearchSourceBuilder.ScriptField> buildScriptFields(IndicesQueriesRegistry queryParsers) {
        if (scriptFields == null) {
            return Collections.emptyList();
        }
        List<SearchSourceBuilder.ScriptField> parsedScriptFields = new ArrayList<>();
        XContentParser parser = createParser(SCRIPT_FIELDS, scriptFields);
        try {
            QueryParseContext queryParseContext = new QueryParseContext(queryParsers, parser, ParseFieldMatcher.STRICT);
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                parsedScriptFields.add(new SearchSourceBuilder.ScriptField(queryParseContext));
            }
        } catch (IOException e) {
            throw ExceptionsHelper.parseException(SCRIPT_FIELDS, e);
        }
        return parsedScriptFields;
    }

    private XContentParser createParser(ParseField parseField, BytesReference bytesReference) {
        try {
            return XContentFactory.xContent(query).createParser(NamedXContentRegistry.EMPTY, query);
        } catch (IOException e) {
            throw ExceptionsHelper.parseException(parseField, e);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(jobId);
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
        out.writeOptionalBytesReference(query);
        out.writeOptionalBytesReference(aggregations);
        out.writeOptionalBytesReference(scriptFields);
        out.writeOptionalVInt(scrollSize);
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
        if (queryDelay != null) {
            builder.field(QUERY_DELAY.getPreferredName(), queryDelay);
        }
        if (frequency != null) {
            builder.field(FREQUENCY.getPreferredName(), frequency);
        }
        if (indexes != null) {
            builder.field(INDEXES.getPreferredName(), indexes);
        }
        if (types != null) {
            builder.field(TYPES.getPreferredName(), types);
        }
        if (query != null) {
            builder.field(QUERY.getPreferredName(), getQueryAsMap());
        }
        if (aggregations != null) {
            builder.field(AGGREGATIONS.getPreferredName(), getAggregationsAsMap());
        }
        if (scriptFields != null) {
            builder.field(SCRIPT_FIELDS.getPreferredName(), getScriptFieldsAsMap());
        }
        if (scrollSize != null) {
            builder.field(SCROLL_SIZE.getPreferredName(), scrollSize);
        }
        return builder;
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

        if (other instanceof SchedulerConfig == false) {
            return false;
        }

        SchedulerConfig that = (SchedulerConfig) other;

        return Objects.equals(this.id, that.id)
                && Objects.equals(this.jobId, that.jobId)
                && Objects.equals(this.frequency, that.frequency)
                && Objects.equals(this.queryDelay, that.queryDelay)
                && Objects.equals(this.indexes, that.indexes)
                && Objects.equals(this.types, that.types)
                && Objects.equals(this.query, that.query)
                && Objects.equals(this.scrollSize, that.scrollSize)
                && Objects.equals(this.aggregations, that.aggregations)
                && Objects.equals(this.scriptFields, that.scriptFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, jobId, frequency, queryDelay, indexes, types, query, scrollSize, aggregations, scriptFields);
    }

    public static class Builder {

        private static final int DEFAULT_SCROLL_SIZE = 1000;
        private static final long DEFAULT_ELASTICSEARCH_QUERY_DELAY = 60L;

        private String id;
        private String jobId;
        private Long queryDelay;
        private Long frequency;
        private List<String> indexes = Collections.emptyList();
        private List<String> types = Collections.emptyList();
        private BytesReference query;
        private BytesReference aggregations;
        private BytesReference scriptFields;
        private Integer scrollSize;

        public Builder() {
            setQueryDelay(DEFAULT_ELASTICSEARCH_QUERY_DELAY);
            setScrollSize(DEFAULT_SCROLL_SIZE);
        }

        public Builder(String id, String jobId) {
            this();
            this.id = ExceptionsHelper.requireNonNull(id, ID.getPreferredName());
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public Builder(SchedulerConfig config) {
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
        }

        public void setId(String schedulerId) {
            id = ExceptionsHelper.requireNonNull(schedulerId, ID.getPreferredName());
        }

        public void setJobId(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public void setIndexes(List<String> indexes) {
            this.indexes = ExceptionsHelper.requireNonNull(indexes, INDEXES.getPreferredName());
        }

        public void setTypes(List<String> types) {
            this.types = ExceptionsHelper.requireNonNull(types, TYPES.getPreferredName());
        }

        public void setQueryDelay(long queryDelay) {
            if (queryDelay < 0) {
                String msg = Messages.getMessage(Messages.SCHEDULER_CONFIG_INVALID_OPTION_VALUE,
                        SchedulerConfig.QUERY_DELAY.getPreferredName(), queryDelay);
                throw new IllegalArgumentException(msg);
            }
            this.queryDelay = queryDelay;
        }

        public void setFrequency(long frequency) {
            if (frequency <= 0) {
                String msg = Messages.getMessage(Messages.SCHEDULER_CONFIG_INVALID_OPTION_VALUE,
                        SchedulerConfig.FREQUENCY.getPreferredName(), frequency);
                throw new IllegalArgumentException(msg);
            }
            this.frequency = frequency;
        }

        private void setQuery(BytesReference query) {
            this.query = Objects.requireNonNull(query);
        }

        public void setQuery(QueryBuilder query) {
            this.query = xContentToBytes(query);
        }

        private BytesReference xContentToBytes(ToXContent value) {
            try {
                XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
                return value.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS).bytes();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void setAggregations(BytesReference aggregations) {
            this.aggregations = Objects.requireNonNull(aggregations);
        }

        public void setAggregations(AggregatorFactories.Builder aggregations) {
            this.aggregations = xContentToBytes(aggregations);
        }

        private void setScriptFields(BytesReference scriptFields) {
            this.scriptFields = Objects.requireNonNull(scriptFields);
        }

        public void setScriptFields(List<SearchSourceBuilder.ScriptField> scriptFields) {
            try {
                XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
                jsonBuilder.startObject();
                for (SearchSourceBuilder.ScriptField scriptField : scriptFields) {
                    scriptField.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
                }
                jsonBuilder.endObject();
                this.scriptFields = jsonBuilder.bytes();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void setScrollSize(int scrollSize) {
            if (scrollSize < 0) {
                String msg = Messages.getMessage(Messages.SCHEDULER_CONFIG_INVALID_OPTION_VALUE,
                        SchedulerConfig.SCROLL_SIZE.getPreferredName(), scrollSize);
                throw new IllegalArgumentException(msg);
            }
            this.scrollSize = scrollSize;
        }

        public SchedulerConfig build() {
            ExceptionsHelper.requireNonNull(id, ID.getPreferredName());
            ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
            if (!PrelertStrings.isValidId(id)) {
                throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_ID, ID.getPreferredName()));
            }
            if (indexes == null || indexes.isEmpty() || indexes.contains(null) || indexes.contains("")) {
                throw invalidOptionValue(INDEXES.getPreferredName(), indexes);
            }
            if (types == null || types.isEmpty() || types.contains(null) || types.contains("")) {
                throw invalidOptionValue(TYPES.getPreferredName(), types);
            }
            return new SchedulerConfig(id, jobId, queryDelay, frequency, indexes, types, query, aggregations, scriptFields, scrollSize);
        }

        private static ElasticsearchException invalidOptionValue(String fieldName, Object value) {
            String msg = Messages.getMessage(Messages.SCHEDULER_CONFIG_INVALID_OPTION_VALUE, fieldName, value);
            throw new IllegalArgumentException(msg);
        }

        private static ElasticsearchException notSupportedValue(ParseField field, String key) {
            String msg = Messages.getMessage(key, field.getPreferredName());
            throw new IllegalArgumentException(msg);
        }

    }

}
