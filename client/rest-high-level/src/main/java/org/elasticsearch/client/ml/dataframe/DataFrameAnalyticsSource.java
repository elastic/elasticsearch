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

package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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

    private static ObjectParser<Builder, Void> PARSER = new ObjectParser<>("data_frame_analytics_source", true, Builder::new);

    static {
        PARSER.declareStringArray(Builder::setIndex, INDEX);
        PARSER.declareObject(Builder::setQueryConfig, (p, c) -> QueryConfig.fromXContent(p), QUERY);
    }

    private final String[] index;
    private final QueryConfig queryConfig;

    private DataFrameAnalyticsSource(String[] index, @Nullable QueryConfig queryConfig) {
        this.index = Objects.requireNonNull(index);
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
        builder.field(INDEX.getPreferredName(), index);
        if (queryConfig != null) {
            builder.field(QUERY.getPreferredName(), queryConfig.getQuery());
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
            && Objects.equals(queryConfig, other.queryConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.asList(index), queryConfig);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class Builder {

        private String[] index;
        private QueryConfig queryConfig;

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

        public DataFrameAnalyticsSource build() {
            return new DataFrameAnalyticsSource(index, queryConfig);
        }
    }
}
