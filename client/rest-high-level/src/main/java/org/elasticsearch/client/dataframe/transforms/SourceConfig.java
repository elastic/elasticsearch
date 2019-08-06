/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.dataframe.transforms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;


/**
 * Class encapsulating all options for a {@link DataFrameTransformConfig} gathering data
 */
public class SourceConfig implements ToXContentObject {

    public static final ParseField QUERY = new ParseField("query");
    public static final ParseField INDEX = new ParseField("index");

    public static final ConstructingObjectParser<SourceConfig, Void> PARSER = new ConstructingObjectParser<>("data_frame_config_source",
    true,
    args -> {
        @SuppressWarnings("unchecked")
        String[] index = ((List<String>)args[0]).toArray(new String[0]);
        // default handling: if the user does not specify a query, we default to match_all
        QueryConfig queryConfig = (QueryConfig) args[1];
        return new SourceConfig(index, queryConfig);
    });
    static {
        PARSER.declareStringArray(constructorArg(), INDEX);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> QueryConfig.fromXContent(p), QUERY);
    }

    private final String[] index;
    private final QueryConfig queryConfig;

    /**
     * Create a new SourceConfig for the provided indices.
     *
     * {@link QueryConfig} defaults to a MatchAll query.
     *
     * @param index Any number of indices. At least one non-null, non-empty, index should be provided
     */
    public SourceConfig(String... index) {
        this.index = index;
        this.queryConfig = null;
    }

    /**
     * Create a new SourceConfig for the provided indices, from which data is gathered with the provided {@link QueryConfig}
     *
     * @param index Any number of indices. At least one non-null, non-empty, index should be provided
     * @param queryConfig A QueryConfig object that contains the desired query. Defaults to MatchAll query.
     */
    SourceConfig(String[] index, QueryConfig queryConfig) {
        this.index = index;
        this.queryConfig = queryConfig;
    }

    public String[] getIndex() {
        return index;
    }

    public QueryConfig getQueryConfig() {
        return queryConfig;
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
        return Arrays.equals(index, that.index) && Objects.equals(queryConfig, that.queryConfig);
    }

    @Override
    public int hashCode(){
        // Using Arrays.hashCode as Objects.hash does not deeply hash nested arrays. Since we are doing Array.equals, this is necessary
        int hash = Arrays.hashCode(index);
        return 31 * hash + (queryConfig == null ? 0 : queryConfig.hashCode());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String[] index;
        private QueryConfig queryConfig;

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

        public SourceConfig build() {
            return new SourceConfig(index, queryConfig);
        }
    }
}
