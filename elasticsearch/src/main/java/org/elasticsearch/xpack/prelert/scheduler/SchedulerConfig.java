/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.scheduler;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.messages.Messages;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;
import org.elasticsearch.xpack.prelert.utils.PrelertStrings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

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
    public static final ParseField RETRIEVE_WHOLE_SOURCE = new ParseField("retrieve_whole_source");
    public static final ParseField SCROLL_SIZE = new ParseField("scroll_size");
    public static final ParseField AGGREGATIONS = new ParseField("aggregations");
    public static final ParseField AGGS = new ParseField("aggs");
    public static final ParseField SCRIPT_FIELDS = new ParseField("script_fields");

    public static final ObjectParser<Builder, ParseFieldMatcherSupplier> PARSER = new ObjectParser<>("schedule_config", Builder::new);

    static {
        PARSER.declareString(Builder::setId, ID);
        PARSER.declareString(Builder::setJobId, Job.ID);
        PARSER.declareStringArray(Builder::setIndexes, INDEXES);
        PARSER.declareStringArray(Builder::setTypes, TYPES);
        PARSER.declareLong(Builder::setQueryDelay, QUERY_DELAY);
        PARSER.declareLong(Builder::setFrequency, FREQUENCY);
        PARSER.declareObject(Builder::setQuery, (p, c) -> {
            try {
                return p.map();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, QUERY);
        PARSER.declareObject(Builder::setAggregations, (p, c) -> {
            try {
                return p.map();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, AGGREGATIONS);
        PARSER.declareObject(Builder::setAggs, (p, c) -> {
            try {
                return p.map();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, AGGS);
        PARSER.declareObject(Builder::setScriptFields, (p, c) -> {
            try {
                return p.map();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, SCRIPT_FIELDS);
        PARSER.declareBoolean(Builder::setRetrieveWholeSource, RETRIEVE_WHOLE_SOURCE);
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
    // NORELEASE: These 4 fields can be reduced to a single
    // SearchSourceBuilder field holding the entire source:
    private final Map<String, Object> query;
    private final Map<String, Object> aggregations;
    private final Map<String, Object> aggs;
    private final Map<String, Object> scriptFields;
    private final Boolean retrieveWholeSource;
    private final Integer scrollSize;

    private SchedulerConfig(String id, String jobId, Long queryDelay, Long frequency, List<String> indexes, List<String> types,
                            Map<String, Object> query, Map<String, Object> aggregations, Map<String, Object> aggs,
                            Map<String, Object> scriptFields, Boolean retrieveWholeSource, Integer scrollSize) {
        this.id = id;
        this.jobId = jobId;
        this.queryDelay = queryDelay;
        this.frequency = frequency;
        this.indexes = indexes;
        this.types = types;
        this.query = query;
        this.aggregations = aggregations;
        this.aggs = aggs;
        this.scriptFields = scriptFields;
        this.retrieveWholeSource = retrieveWholeSource;
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
        if (in.readBoolean()) {
            this.query = in.readMap();
        } else {
            this.query = null;
        }
        if (in.readBoolean()) {
            this.aggregations = in.readMap();
        } else {
            this.aggregations = null;
        }
        if (in.readBoolean()) {
            this.aggs = in.readMap();
        } else {
            this.aggs = null;
        }
        if (in.readBoolean()) {
            this.scriptFields = in.readMap();
        } else {
            this.scriptFields = null;
        }
        this.retrieveWholeSource = in.readOptionalBoolean();
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

    /**
     * For the ELASTICSEARCH data source only, the Elasticsearch query DSL
     * representing the query to submit to Elasticsearch to get the input data.
     * This should not include time bounds, as these are added separately. This
     * class does not attempt to interpret the query. The map will be converted
     * back to an arbitrary JSON object.
     *
     * @return The search query, or <code>null</code> if not set.
     */
    public Map<String, Object> getQuery() {
        return this.query;
    }

    /**
     * For the ELASTICSEARCH data source only, should the whole _source document
     * be retrieved for analysis, or just the analysis fields?
     *
     * @return Should the whole of _source be retrieved? (<code>null</code> if
     *         not set.)
     */
    public Boolean getRetrieveWholeSource() {
        return this.retrieveWholeSource;
    }

    /**
     * For the ELASTICSEARCH data source only, get the size of documents to be
     * retrieved from each shard via a scroll search
     *
     * @return The size of documents to be retrieved from each shard via a
     *         scroll search
     */
    public Integer getScrollSize() {
        return this.scrollSize;
    }

    /**
     * For the ELASTICSEARCH data source only, optional Elasticsearch
     * script_fields to add to the search to be submitted to Elasticsearch to
     * get the input data. This class does not attempt to interpret the script
     * fields. The map will be converted back to an arbitrary JSON object.
     *
     * @return The script fields, or <code>null</code> if not set.
     */
    public Map<String, Object> getScriptFields() {
        return this.scriptFields;
    }

    /**
     * For the ELASTICSEARCH data source only, optional Elasticsearch
     * aggregations to apply to the search to be submitted to Elasticsearch to
     * get the input data. This class does not attempt to interpret the
     * aggregations. The map will be converted back to an arbitrary JSON object.
     * Synonym for {@link #getAggs()} (like Elasticsearch).
     *
     * @return The aggregations, or <code>null</code> if not set.
     */
    public Map<String, Object> getAggregations() {
        return this.aggregations;
    }

    /**
     * For the ELASTICSEARCH data source only, optional Elasticsearch
     * aggregations to apply to the search to be submitted to Elasticsearch to
     * get the input data. This class does not attempt to interpret the
     * aggregations. The map will be converted back to an arbitrary JSON object.
     * Synonym for {@link #getAggregations()} (like Elasticsearch).
     *
     * @return The aggregations, or <code>null</code> if not set.
     */
    public Map<String, Object> getAggs() {
        return this.aggs;
    }

    /**
     * Convenience method to get either aggregations or aggs.
     *
     * @return The aggregations (whether initially specified in aggregations or
     *         aggs), or <code>null</code> if neither are set.
     */
    public Map<String, Object> getAggregationsOrAggs() {
        return (this.aggregations != null) ? this.aggregations : this.aggs;
    }

    /**
     * Build the list of fields expected in the output from aggregations
     * submitted to Elasticsearch.
     *
     * @return The list of fields, or empty list if there are no aggregations.
     */
    public List<String> buildAggregatedFieldList() {
        Map<String, Object> aggs = getAggregationsOrAggs();
        if (aggs == null) {
            return Collections.emptyList();
        }

        SortedMap<Integer, String> orderedFields = new TreeMap<>();

        scanSubLevel(aggs, 0, orderedFields);

        return new ArrayList<>(orderedFields.values());
    }

    @SuppressWarnings("unchecked")
    private void scanSubLevel(Map<String, Object> subLevel, int depth, SortedMap<Integer, String> orderedFields) {
        for (Map.Entry<String, Object> entry : subLevel.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map<?, ?>) {
                scanSubLevel((Map<String, Object>) value, depth + 1, orderedFields);
            } else if (value instanceof String && FIELD.equals(entry.getKey())) {
                orderedFields.put(depth, (String) value);
            }
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
        if (query != null) {
            out.writeBoolean(true);
            out.writeMap(query);
        } else {
            out.writeBoolean(false);
        }
        if (aggregations != null) {
            out.writeBoolean(true);
            out.writeMap(aggregations);
        } else {
            out.writeBoolean(false);
        }
        if (aggs != null) {
            out.writeBoolean(true);
            out.writeMap(aggs);
        } else {
            out.writeBoolean(false);
        }
        if (scriptFields != null) {
            out.writeBoolean(true);
            out.writeMap(scriptFields);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalBoolean(retrieveWholeSource);
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
            builder.field(QUERY.getPreferredName(), query);
        }
        if (aggregations != null) {
            builder.field(AGGREGATIONS.getPreferredName(), aggregations);
        }
        if (aggs != null) {
            builder.field(AGGS.getPreferredName(), aggs);
        }
        if (scriptFields != null) {
            builder.field(SCRIPT_FIELDS.getPreferredName(), scriptFields);
        }
        if (retrieveWholeSource != null) {
            builder.field(RETRIEVE_WHOLE_SOURCE.getPreferredName(), retrieveWholeSource);
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
                && Objects.equals(this.types, that.types) && Objects.equals(this.query, that.query)
                && Objects.equals(this.retrieveWholeSource, that.retrieveWholeSource) && Objects.equals(this.scrollSize, that.scrollSize)
                && Objects.equals(this.getAggregationsOrAggs(), that.getAggregationsOrAggs())
                && Objects.equals(this.scriptFields, that.scriptFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, jobId, frequency, queryDelay, indexes, types, query, retrieveWholeSource, scrollSize,
                getAggregationsOrAggs(), scriptFields);
    }

    public static class Builder {

        private static final int DEFAULT_SCROLL_SIZE = 1000;
        private static final long DEFAULT_ELASTICSEARCH_QUERY_DELAY = 60L;

        /**
         * The default query for elasticsearch searches
         */
        private static final String MATCH_ALL_ES_QUERY = "match_all";

        private String id;
        private String jobId;
        private Long queryDelay;
        private Long frequency;
        private List<String> indexes = Collections.emptyList();
        private List<String> types = Collections.emptyList();
        // NORELEASE: use Collections.emptyMap() instead of null as initial
        // value:
        // NORELEASE: Use SearchSourceBuilder
        private Map<String, Object> query = null;
        private Map<String, Object> aggregations = null;
        private Map<String, Object> aggs = null;
        private Map<String, Object> scriptFields = null;
        private Boolean retrieveWholeSource;
        private Integer scrollSize;

        public Builder() {
            Map<String, Object> query = new HashMap<>();
            query.put(MATCH_ALL_ES_QUERY, new HashMap<String, Object>());
            setQuery(query);
            setQueryDelay(DEFAULT_ELASTICSEARCH_QUERY_DELAY);
            setRetrieveWholeSource(false);
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
            this.aggs = config.aggs;
            this.scriptFields = config.scriptFields;
            this.retrieveWholeSource = config.retrieveWholeSource;
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

        public void setQuery(Map<String, Object> query) {
            // NORELEASE: make use of Collections.unmodifiableMap(...)
            this.query = Objects.requireNonNull(query);
        }

        public void setAggregations(Map<String, Object> aggregations) {
            // NORELEASE: make use of Collections.unmodifiableMap(...)
            this.aggregations = Objects.requireNonNull(aggregations);
        }

        public void setAggs(Map<String, Object> aggs) {
            // NORELEASE: make use of Collections.unmodifiableMap(...)
            this.aggs = Objects.requireNonNull(aggs);
        }

        public void setScriptFields(Map<String, Object> scriptFields) {
            // NORELEASE: make use of Collections.unmodifiableMap(...)
            this.scriptFields = Objects.requireNonNull(scriptFields);
        }

        public void setRetrieveWholeSource(boolean retrieveWholeSource) {
            this.retrieveWholeSource = retrieveWholeSource;
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
            if (aggregations != null && aggs != null) {
                String msg = Messages.getMessage(Messages.SCHEDULER_CONFIG_MULTIPLE_AGGREGATIONS);
                throw new IllegalArgumentException(msg);
            }
            if (Boolean.TRUE.equals(retrieveWholeSource)) {
                if (scriptFields != null) {
                    throw notSupportedValue(SCRIPT_FIELDS, Messages.SCHEDULER_CONFIG_FIELD_NOT_SUPPORTED);
                }
            }
            return new SchedulerConfig(id, jobId, queryDelay, frequency, indexes, types, query, aggregations, aggs, scriptFields,
                    retrieveWholeSource, scrollSize);
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
