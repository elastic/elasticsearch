/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DataFrameAnalyticsSource implements ToXContentObject {

    public static DataFrameAnalyticsSource fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private static final ParseField INDEX = new ParseField("index");
    private static final ParseField QUERY = new ParseField("query");
    public static final ParseField _SOURCE = new ParseField("_source");

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("data_frame_analytics_source", true, Builder::new);

    static {
        PARSER.declareStringArray(Builder::setIndex, INDEX);
        PARSER.declareObject(Builder::setQueryConfig, (p, c) -> QueryConfig.fromXContent(p), QUERY);
        PARSER.declareField(Builder::setSourceFiltering,
            (p, c) -> FetchSourceContext.fromXContent(p),
            _SOURCE,
            ObjectParser.ValueType.OBJECT_ARRAY_BOOLEAN_OR_STRING);
        PARSER.declareObject(Builder::setRuntimeMappings, (p, c) -> p.map(), SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD);
    }

    private final String[] index;
    private final QueryConfig queryConfig;
    private final FetchSourceContext sourceFiltering;
    private final Map<String, Object> runtimeMappings;

    private DataFrameAnalyticsSource(String[] index, @Nullable QueryConfig queryConfig, @Nullable FetchSourceContext sourceFiltering,
                                     @Nullable Map<String, Object> runtimeMappings) {
        this.index = Objects.requireNonNull(index);
        this.queryConfig = queryConfig;
        this.sourceFiltering = sourceFiltering;
        this.runtimeMappings = runtimeMappings;
    }

    public String[] getIndex() {
        return index;
    }

    public QueryConfig getQueryConfig() {
        return queryConfig;
    }

    public FetchSourceContext getSourceFiltering() {
        return sourceFiltering;
    }

    public Map<String, Object> getRuntimeMappings() {
        return runtimeMappings;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX.getPreferredName(), index);
        if (queryConfig != null) {
            builder.field(QUERY.getPreferredName(), queryConfig.getQuery());
        }
        if (sourceFiltering != null) {
            builder.field(_SOURCE.getPreferredName(), sourceFiltering);
        }
        if (runtimeMappings != null) {
            builder.field(SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD.getPreferredName(), runtimeMappings);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataFrameAnalyticsSource other = (DataFrameAnalyticsSource) o;
        return Arrays.equals(index, other.index)
            && Objects.equals(queryConfig, other.queryConfig)
            && Objects.equals(sourceFiltering, other.sourceFiltering)
            && Objects.equals(runtimeMappings, other.runtimeMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.asList(index), queryConfig, sourceFiltering, runtimeMappings);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class Builder {

        private String[] index;
        private QueryConfig queryConfig;
        private FetchSourceContext sourceFiltering;
        private Map<String, Object> runtimeMappings;

        private Builder() {}

        public Builder setIndex(String... index) {
            this.index = index;
            return this;
        }

        public Builder setIndex(List<String> index) {
            this.index = index.toArray(new String[0]);
            return this;
        }

        public Builder setQueryConfig(QueryConfig queryConfig) {
            this.queryConfig = queryConfig;
            return this;
        }

        public Builder setSourceFiltering(FetchSourceContext sourceFiltering) {
            this.sourceFiltering = sourceFiltering;
            return this;
        }

        public Builder setRuntimeMappings(Map<String, Object> runtimeMappings) {
            this.runtimeMappings = runtimeMappings;
            return this;
        }

        public DataFrameAnalyticsSource build() {
            return new DataFrameAnalyticsSource(index, queryConfig, sourceFiltering, runtimeMappings);
        }
    }
}
