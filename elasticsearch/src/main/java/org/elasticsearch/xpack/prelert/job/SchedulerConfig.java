/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.prelert.job.messages.Messages;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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

    // NORELEASE: no camel casing:
    public static final ParseField DATA_SOURCE = new ParseField("data_source");
    public static final ParseField QUERY_DELAY = new ParseField("query_delay");
    public static final ParseField FREQUENCY = new ParseField("frequency");
    public static final ParseField FILE_PATH = new ParseField("file_path");
    public static final ParseField TAIL_FILE = new ParseField("tail_file");
    public static final ParseField BASE_URL = new ParseField("base_url");
    public static final ParseField USERNAME = new ParseField("username");
    public static final ParseField PASSWORD = new ParseField("password");
    public static final ParseField ENCRYPTED_PASSWORD = new ParseField("encrypted_password");
    public static final ParseField INDEXES = new ParseField("indexes");
    public static final ParseField TYPES = new ParseField("types");
    public static final ParseField QUERY = new ParseField("query");
    public static final ParseField RETRIEVE_WHOLE_SOURCE = new ParseField("retrieve_whole_source");
    public static final ParseField SCROLL_SIZE = new ParseField("scroll_size");
    public static final ParseField AGGREGATIONS = new ParseField("aggregations");
    public static final ParseField AGGS = new ParseField("aggs");
    /**
     * Named to match Elasticsearch, hence lowercase_with_underscores instead of
     * camelCase
     */
    public static final ParseField SCRIPT_FIELDS = new ParseField("script_fields");

    public static final ConstructingObjectParser<SchedulerConfig.Builder, ParseFieldMatcherSupplier> PARSER =
            new ConstructingObjectParser<>("schedule_config", a -> new SchedulerConfig.Builder((DataSource) a[0]));

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return DataSource.readFromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, DATA_SOURCE, ObjectParser.ValueType.STRING);
        PARSER.declareLong(Builder::setQueryDelay, QUERY_DELAY);
        PARSER.declareLong(Builder::setFrequency, FREQUENCY);
        PARSER.declareString(Builder::setFilePath, FILE_PATH);
        PARSER.declareBoolean(Builder::setTailFile, TAIL_FILE);
        PARSER.declareString(Builder::setUsername, USERNAME);
        PARSER.declareString(Builder::setPassword, PASSWORD);
        PARSER.declareString(Builder::setEncryptedPassword, ENCRYPTED_PASSWORD);
        PARSER.declareString(Builder::setBaseUrl, BASE_URL);
        PARSER.declareStringArray(Builder::setIndexes, INDEXES);
        PARSER.declareStringArray(Builder::setTypes, TYPES);
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

    // NORELEASE: please use primitives where possible here:
    private final DataSource dataSource;

    /**
     * The delay in seconds before starting to query a period of time
     */
    private final Long queryDelay;

    /**
     * The frequency in seconds with which queries are executed
     */
    private final Long frequency;

    /**
     * These values apply to the FILE data source
     */
    private final String filePath;
    private final Boolean tailFile;

    /**
     * Used for data sources that require credentials. May be null in the case
     * where credentials are sometimes needed and sometimes not (e.g.
     * Elasticsearch).
     */
    private final String username;
    private final String password;
    private final String encryptedPassword;

    /**
     * These values apply to the ELASTICSEARCH data source
     */
    private final String baseUrl;
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

    private SchedulerConfig(DataSource dataSource, Long queryDelay, Long frequency, String filePath, Boolean tailFile, String username,
            String password, String encryptedPassword, String baseUrl, List<String> indexes, List<String> types, Map<String, Object> query,
            Map<String, Object> aggregations, Map<String, Object> aggs, Map<String, Object> scriptFields, Boolean retrieveWholeSource,
            Integer scrollSize) {
        this.dataSource = dataSource;
        this.queryDelay = queryDelay;
        this.frequency = frequency;
        this.filePath = filePath;
        this.tailFile = tailFile;
        this.username = username;
        this.password = password;
        this.encryptedPassword = encryptedPassword;
        this.baseUrl = baseUrl;
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
        this.dataSource = DataSource.readFromStream(in);
        this.queryDelay = in.readOptionalLong();
        this.frequency = in.readOptionalLong();
        this.filePath = in.readOptionalString();
        this.tailFile = in.readOptionalBoolean();
        this.username = in.readOptionalString();
        this.password = in.readOptionalString();
        this.encryptedPassword = in.readOptionalString();
        this.baseUrl = in.readOptionalString();
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

    /**
     * The data source that the scheduler is to pull data from.
     *
     * @return The data source.
     */
    public DataSource getDataSource() {
        return this.dataSource;
    }

    public Long getQueryDelay() {
        return this.queryDelay;
    }

    public Long getFrequency() {
        return this.frequency;
    }

    /**
     * For the FILE data source only, the path to the file.
     *
     * @return The path to the file, or <code>null</code> if not set.
     */
    public String getFilePath() {
        return this.filePath;
    }

    /**
     * For the FILE data source only, should the file be tailed? If not it will
     * just be read from once.
     *
     * @return Should the file be tailed? (<code>null</code> if not set.)
     */
    public Boolean getTailFile() {
        return this.tailFile;
    }

    /**
     * For the ELASTICSEARCH data source only, the base URL to connect to
     * Elasticsearch on.
     *
     * @return The URL, or <code>null</code> if not set.
     */
    public String getBaseUrl() {
        return this.baseUrl;
    }

    /**
     * The username to use to connect to the data source (if any).
     *
     * @return The username, or <code>null</code> if not set.
     */
    public String getUsername() {
        return this.username;
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
     * The encrypted password to use to connect to the data source (if any). A
     * class outside this package is responsible for encrypting and decrypting
     * the password.
     *
     * @return The password, or <code>null</code> if not set.
     */
    public String getEncryptedPassword() {
        return encryptedPassword;
    }

    /**
     * The plain text password to use to connect to the data source (if any).
     * This is likely to return <code>null</code> most of the time, as the
     * intention is that it is only present it initial configurations, and gets
     * replaced with an encrypted password as soon as possible after receipt.
     *
     * @return The password, or <code>null</code> if not set.
     */
    public String getPassword() {
        return password;
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
        dataSource.writeTo(out);
        out.writeOptionalLong(queryDelay);
        out.writeOptionalLong(frequency);
        out.writeOptionalString(filePath);
        out.writeOptionalBoolean(tailFile);
        out.writeOptionalString(username);
        out.writeOptionalString(password);
        out.writeOptionalString(encryptedPassword);
        out.writeOptionalString(baseUrl);
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
        builder.field(DATA_SOURCE.getPreferredName(), dataSource.name().toUpperCase(Locale.ROOT));
        if (queryDelay != null) {
            builder.field(QUERY_DELAY.getPreferredName(), queryDelay);
        }
        if (frequency != null) {
            builder.field(FREQUENCY.getPreferredName(), frequency);
        }
        if (filePath != null) {
            builder.field(FILE_PATH.getPreferredName(), filePath);
        }
        if (tailFile != null) {
            builder.field(TAIL_FILE.getPreferredName(), tailFile);
        }
        if (username != null) {
            builder.field(USERNAME.getPreferredName(), username);
        }
        if (password != null) {
            builder.field(PASSWORD.getPreferredName(), password);
        }
        if (encryptedPassword != null) {
            builder.field(ENCRYPTED_PASSWORD.getPreferredName(), encryptedPassword);
        }
        if (baseUrl != null) {
            builder.field(BASE_URL.getPreferredName(), baseUrl);
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
        builder.endObject();
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

        return Objects.equals(this.dataSource, that.dataSource) && Objects.equals(this.frequency, that.frequency)
                && Objects.equals(this.queryDelay, that.queryDelay) && Objects.equals(this.filePath, that.filePath)
                && Objects.equals(this.tailFile, that.tailFile) && Objects.equals(this.baseUrl, that.baseUrl)
                && Objects.equals(this.username, that.username) && Objects.equals(this.password, that.password)
                && Objects.equals(this.encryptedPassword, that.encryptedPassword) && Objects.equals(this.indexes, that.indexes)
                && Objects.equals(this.types, that.types) && Objects.equals(this.query, that.query)
                && Objects.equals(this.retrieveWholeSource, that.retrieveWholeSource) && Objects.equals(this.scrollSize, that.scrollSize)
                && Objects.equals(this.getAggregationsOrAggs(), that.getAggregationsOrAggs())
                && Objects.equals(this.scriptFields, that.scriptFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.dataSource, frequency, queryDelay, this.filePath, tailFile, baseUrl, username, password, encryptedPassword,
                this.indexes, types, query, retrieveWholeSource, scrollSize, getAggregationsOrAggs(), this.scriptFields);
    }

    /**
     * Enum of the acceptable data sources.
     */
    public enum DataSource implements Writeable {

        FILE, ELASTICSEARCH;

        /**
         * Case-insensitive from string method. Works with ELASTICSEARCH,
         * Elasticsearch, ElasticSearch, etc.
         *
         * @param value
         *            String representation
         * @return The data source
         */
        public static DataSource readFromString(String value) {
            String valueUpperCase = value.toUpperCase(Locale.ROOT);
            return DataSource.valueOf(valueUpperCase);
        }

        public static DataSource readFromStream(StreamInput in) throws IOException {
            int ordinal = in.readVInt();
            if (ordinal < 0 || ordinal >= values().length) {
                throw new IOException("Unknown Operator ordinal [" + ordinal + "]");
            }
            return values()[ordinal];
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(ordinal());
        }
    }

    public static class Builder {

        private static final int DEFAULT_SCROLL_SIZE = 1000;
        private static final long DEFAULT_ELASTICSEARCH_QUERY_DELAY = 60L;

        /**
         * The default query for elasticsearch searches
         */
        private static final String MATCH_ALL_ES_QUERY = "match_all";

        private final DataSource dataSource;
        private Long queryDelay;
        private Long frequency;
        private String filePath;
        private Boolean tailFile;
        private String username;
        private String password;
        private String encryptedPassword;
        private String baseUrl;
        // NORELEASE: use Collections.emptyList() instead of null as initial
        // value:
        private List<String> indexes = null;
        private List<String> types = null;
        // NORELEASE: use Collections.emptyMap() instead of null as initial
        // value:
        // NORELEASE: Use SearchSourceBuilder
        private Map<String, Object> query = null;
        private Map<String, Object> aggregations = null;
        private Map<String, Object> aggs = null;
        private Map<String, Object> scriptFields = null;
        private Boolean retrieveWholeSource;
        private Integer scrollSize;

        // NORELEASE: figure out what the required fields are and made part of
        // the only public constructor
        public Builder(DataSource dataSource) {
            this.dataSource = Objects.requireNonNull(dataSource);
            switch (dataSource) {
            case FILE:
                setTailFile(false);
                break;
            case ELASTICSEARCH:
                Map<String, Object> query = new HashMap<>();
                query.put(MATCH_ALL_ES_QUERY, new HashMap<String, Object>());
                setQuery(query);
                setQueryDelay(DEFAULT_ELASTICSEARCH_QUERY_DELAY);
                setRetrieveWholeSource(false);
                setScrollSize(DEFAULT_SCROLL_SIZE);
                break;
            default:
                throw new UnsupportedOperationException("unsupported datasource " + dataSource);
            }
        }

        public Builder(SchedulerConfig config) {
            this.dataSource = config.dataSource;
            this.queryDelay = config.queryDelay;
            this.frequency = config.frequency;
            this.filePath = config.filePath;
            this.tailFile = config.tailFile;
            this.username = config.username;
            this.password = config.password;
            this.encryptedPassword = config.encryptedPassword;
            this.baseUrl = config.baseUrl;
            this.indexes = config.indexes;
            this.types = config.types;
            this.query = config.query;
            this.aggregations = config.aggregations;
            this.aggs = config.aggs;
            this.scriptFields = config.scriptFields;
            this.retrieveWholeSource = config.retrieveWholeSource;
            this.scrollSize = config.scrollSize;
        }

        public void setQueryDelay(long queryDelay) {
            if (queryDelay < 0) {
                String msg = Messages.getMessage(Messages.JOB_CONFIG_SCHEDULER_INVALID_OPTION_VALUE,
                        SchedulerConfig.QUERY_DELAY.getPreferredName(), queryDelay);
                throw new IllegalArgumentException(msg);
            }
            this.queryDelay = queryDelay;
        }

        public void setFrequency(long frequency) {
            if (frequency <= 0) {
                String msg = Messages.getMessage(Messages.JOB_CONFIG_SCHEDULER_INVALID_OPTION_VALUE,
                        SchedulerConfig.FREQUENCY.getPreferredName(), frequency);
                throw new IllegalArgumentException(msg);
            }
            this.frequency = frequency;
        }

        public void setFilePath(String filePath) {
            this.filePath = Objects.requireNonNull(filePath);
        }

        public void setTailFile(boolean tailFile) {
            this.tailFile = tailFile;
        }

        public void setUsername(String username) {
            this.username = Objects.requireNonNull(username);
        }

        public void setPassword(String password) {
            this.password = Objects.requireNonNull(password);
        }

        public void setEncryptedPassword(String encryptedPassword) {
            this.encryptedPassword = Objects.requireNonNull(encryptedPassword);
        }

        public void setBaseUrl(String baseUrl) {
            this.baseUrl = Objects.requireNonNull(baseUrl);
        }

        public void setIndexes(List<String> indexes) {
            // NORELEASE: make use of Collections.unmodifiableList(...)
            this.indexes = Objects.requireNonNull(indexes);
        }

        public void setTypes(List<String> types) {
            // NORELEASE: make use of Collections.unmodifiableList(...)
            this.types = Objects.requireNonNull(types);
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
                String msg = Messages.getMessage(Messages.JOB_CONFIG_SCHEDULER_INVALID_OPTION_VALUE,
                        SchedulerConfig.SCROLL_SIZE.getPreferredName(), scrollSize);
                throw new IllegalArgumentException(msg);
            }
            this.scrollSize = scrollSize;
        }

        public DataSource getDataSource() {
            return dataSource;
        }

        public Long getQueryDelay() {
            return queryDelay;
        }

        public Long getFrequency() {
            return frequency;
        }

        public String getFilePath() {
            return filePath;
        }

        public Boolean getTailFile() {
            return tailFile;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public String getEncryptedPassword() {
            return encryptedPassword;
        }

        public String getBaseUrl() {
            return baseUrl;
        }

        public List<String> getIndexes() {
            return indexes;
        }

        public List<String> getTypes() {
            return types;
        }

        public Map<String, Object> getQuery() {
            return query;
        }

        public Map<String, Object> getAggregations() {
            return aggregations;
        }

        public Map<String, Object> getAggs() {
            return aggs;
        }

        /**
         * Convenience method to get either aggregations or aggs.
         *
         * @return The aggregations (whether initially specified in aggregations
         *         or aggs), or <code>null</code> if neither are set.
         */
        public Map<String, Object> getAggregationsOrAggs() {
            return (this.aggregations != null) ? this.aggregations : this.aggs;
        }

        public Map<String, Object> getScriptFields() {
            return scriptFields;
        }

        public Boolean getRetrieveWholeSource() {
            return retrieveWholeSource;
        }

        public Integer getScrollSize() {
            return scrollSize;
        }

        public SchedulerConfig build() {
            switch (dataSource) {
            case FILE:
                if (Strings.hasLength(filePath) == false) {
                    throw invalidOptionValue(FILE_PATH.getPreferredName(), filePath);
                }
                if (baseUrl != null) {
                    throw notSupportedValue(BASE_URL, dataSource, Messages.JOB_CONFIG_SCHEDULER_FIELD_NOT_SUPPORTED);
                }
                if (username != null) {
                    throw notSupportedValue(USERNAME, dataSource, Messages.JOB_CONFIG_SCHEDULER_FIELD_NOT_SUPPORTED);
                }
                if (password != null) {
                    throw notSupportedValue(PASSWORD, dataSource, Messages.JOB_CONFIG_SCHEDULER_FIELD_NOT_SUPPORTED);
                }
                if (encryptedPassword != null) {
                    throw notSupportedValue(ENCRYPTED_PASSWORD, dataSource, Messages.JOB_CONFIG_SCHEDULER_FIELD_NOT_SUPPORTED);
                }
                if (indexes != null) {
                    throw notSupportedValue(INDEXES, dataSource, Messages.JOB_CONFIG_SCHEDULER_FIELD_NOT_SUPPORTED);
                }
                if (types != null) {
                    throw notSupportedValue(TYPES, dataSource, Messages.JOB_CONFIG_SCHEDULER_FIELD_NOT_SUPPORTED);
                }
                if (retrieveWholeSource != null) {
                    throw notSupportedValue(RETRIEVE_WHOLE_SOURCE, dataSource, Messages.JOB_CONFIG_SCHEDULER_FIELD_NOT_SUPPORTED);
                }
                if (aggregations != null) {
                    throw notSupportedValue(AGGREGATIONS, dataSource, Messages.JOB_CONFIG_SCHEDULER_FIELD_NOT_SUPPORTED);
                }
                if (query != null) {
                    throw notSupportedValue(QUERY, dataSource, Messages.JOB_CONFIG_SCHEDULER_FIELD_NOT_SUPPORTED);
                }
                if (scriptFields != null) {
                    throw notSupportedValue(SCRIPT_FIELDS, dataSource, Messages.JOB_CONFIG_SCHEDULER_FIELD_NOT_SUPPORTED);
                }
                if (scrollSize != null) {
                    throw notSupportedValue(SCROLL_SIZE, dataSource, Messages.JOB_CONFIG_SCHEDULER_FIELD_NOT_SUPPORTED);
                }
                break;
            case ELASTICSEARCH:
                try {
                    new URL(baseUrl);
                } catch (MalformedURLException e) {
                    throw invalidOptionValue(BASE_URL.getPreferredName(), baseUrl);
                }
                boolean isNoPasswordSet = password == null && encryptedPassword == null;
                boolean isMultiplePasswordSet = password != null && encryptedPassword != null;
                if ((username != null && isNoPasswordSet) || (isNoPasswordSet == false && username == null)) {
                    String msg = Messages.getMessage(Messages.JOB_CONFIG_SCHEDULER_INCOMPLETE_CREDENTIALS);
                    throw new IllegalArgumentException(msg);
                }
                if (isMultiplePasswordSet) {
                    String msg = Messages.getMessage(Messages.JOB_CONFIG_SCHEDULER_MULTIPLE_PASSWORDS);
                    throw new IllegalArgumentException(msg);
                }
                if (indexes == null || indexes.isEmpty() || indexes.contains(null) || indexes.contains("")) {
                    throw invalidOptionValue(INDEXES.getPreferredName(), indexes);
                }
                if (types == null || types.isEmpty() || types.contains(null) || types.contains("")) {
                    throw invalidOptionValue(TYPES.getPreferredName(), types);
                }
                if (aggregations != null && aggs != null) {
                    String msg = Messages.getMessage(Messages.JOB_CONFIG_SCHEDULER_MULTIPLE_AGGREGATIONS);
                    throw new IllegalArgumentException(msg);
                }
                if (Boolean.TRUE.equals(retrieveWholeSource)) {
                    if (scriptFields != null) {
                        throw notSupportedValue(SCRIPT_FIELDS, dataSource, Messages.JOB_CONFIG_SCHEDULER_FIELD_NOT_SUPPORTED);
                    }
                }
                if (filePath != null) {
                    throw notSupportedValue(FILE_PATH, dataSource, Messages.JOB_CONFIG_SCHEDULER_FIELD_NOT_SUPPORTED);
                }
                if (tailFile != null) {
                    throw notSupportedValue(TAIL_FILE, dataSource, Messages.JOB_CONFIG_SCHEDULER_FIELD_NOT_SUPPORTED);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected datasource [" + dataSource + "]");
            }
            return new SchedulerConfig(dataSource, queryDelay, frequency, filePath, tailFile, username, password, encryptedPassword,
                    baseUrl, indexes, types, query, aggregations, aggs, scriptFields, retrieveWholeSource, scrollSize);
        }

        private static ElasticsearchException invalidOptionValue(String fieldName, Object value) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_SCHEDULER_INVALID_OPTION_VALUE, fieldName, value);
            throw new IllegalArgumentException(msg);
        }

        private static ElasticsearchException notSupportedValue(ParseField field, DataSource dataSource, String key) {
            String msg = Messages.getMessage(key, field.getPreferredName(), dataSource.toString());
            throw new IllegalArgumentException(msg);
        }

    }

}
