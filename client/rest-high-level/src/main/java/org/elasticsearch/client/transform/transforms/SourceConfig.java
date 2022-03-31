/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Class encapsulating all options for a {@link TransformConfig} gathering data
 */
public class SourceConfig implements ToXContentObject {

    public static final ParseField QUERY = new ParseField("query");
    public static final ParseField INDEX = new ParseField("index");

    public static final ConstructingObjectParser<SourceConfig, Void> PARSER = new ConstructingObjectParser<>(
        "transform_config_source",
        true,
        args -> {
            @SuppressWarnings("unchecked")
            String[] index = ((List<String>) args[0]).toArray(new String[0]);
            // default handling: if the user does not specify a query, we default to match_all
            QueryConfig queryConfig = (QueryConfig) args[1];
            @SuppressWarnings("unchecked")
            Map<String, Object> runtimeMappings = (Map<String, Object>) args[2];
            return new SourceConfig(index, queryConfig, runtimeMappings);
        }
    );
    static {
        PARSER.declareStringArray(constructorArg(), INDEX);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> QueryConfig.fromXContent(p), QUERY);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD);
    }

    private final String[] index;
    private final QueryConfig queryConfig;
    private final Map<String, Object> runtimeMappings;

    /**
     * Create a new SourceConfig for the provided indices.
     *
     * {@link QueryConfig} defaults to a MatchAll query.
     *
     * @param index Any number of indices. At least one non-null, non-empty, index should be provided
     */
    public SourceConfig(String... index) {
        this(index, null, null);
    }

    /**
     * Create a new SourceConfig for the provided indices, from which data is gathered with the provided {@link QueryConfig}
     *
     * @param index Any number of indices. At least one non-null, non-empty, index should be provided
     * @param queryConfig A QueryConfig object that contains the desired query. Defaults to MatchAll query.
     * @param runtimeMappings Search-time runtime fields that can be used by the transform
     */
    SourceConfig(String[] index, QueryConfig queryConfig, Map<String, Object> runtimeMappings) {
        this.index = index;
        this.queryConfig = queryConfig;
        this.runtimeMappings = runtimeMappings;
    }

    public String[] getIndex() {
        return index;
    }

    public QueryConfig getQueryConfig() {
        return queryConfig;
    }

    public Map<String, Object> getRuntimeMappings() {
        return runtimeMappings;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (index != null) {
            builder.array(INDEX.getPreferredName(), index);
        }
        if (queryConfig != null) {
            builder.field(QUERY.getPreferredName(), queryConfig);
        }
        if (runtimeMappings != null) {
            builder.field(SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD.getPreferredName(), runtimeMappings);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other == null || other.getClass() != getClass()) {
            return false;
        }

        SourceConfig that = (SourceConfig) other;
        return Arrays.equals(index, that.index)
            && Objects.equals(queryConfig, that.queryConfig)
            && Objects.equals(runtimeMappings, that.runtimeMappings);
    }

    @Override
    public int hashCode() {
        // Using Arrays.hashCode as Objects.hash does not deeply hash nested arrays. Since we are doing Array.equals, this is necessary
        int indexArrayHash = Arrays.hashCode(index);
        return Objects.hash(indexArrayHash, queryConfig, runtimeMappings);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String[] index;
        private QueryConfig queryConfig;
        private Map<String, Object> runtimeMappings;

        /**
         * Sets what indices from which to fetch data
         * @param index The indices from which to fetch data
         * @return The {@link Builder} with indices set
         */
        public Builder setIndex(String... index) {
            this.index = index;
            return this;
        }

        /**
         * Sets the {@link QueryConfig} object that references the desired query to use when fetching the data
         * @param queryConfig The {@link QueryConfig} to use when fetching data
         * @return The {@link Builder} with queryConfig set
         */
        public Builder setQueryConfig(QueryConfig queryConfig) {
            this.queryConfig = queryConfig;
            return this;
        }

        /**
         * Sets the query to use when fetching the data. Convenience method for {@link #setQueryConfig(QueryConfig)}
         * @param query The {@link QueryBuilder} to use when fetch data (overwrites the {@link QueryConfig})
         * @return The {@link Builder} with queryConfig set
         */
        public Builder setQuery(QueryBuilder query) {
            return this.setQueryConfig(new QueryConfig(query));
        }

        public Builder setRuntimeMappings(Map<String, Object> runtimeMappings) {
            this.runtimeMappings = runtimeMappings;
            return this;
        }

        public SourceConfig build() {
            return new SourceConfig(index, queryConfig, runtimeMappings);
        }
    }
}
