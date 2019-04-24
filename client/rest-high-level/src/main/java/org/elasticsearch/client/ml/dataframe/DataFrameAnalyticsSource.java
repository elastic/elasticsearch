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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class DataFrameAnalyticsSource implements ToXContentObject {

    public static DataFrameAnalyticsSource fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static final ParseField INDEX = new ParseField("index");
    private static final ParseField QUERY = new ParseField("query");

    private static ConstructingObjectParser<DataFrameAnalyticsSource, Void> PARSER =
        new ConstructingObjectParser<>("data_frame_analytics_source", true,
            (args) -> {
                String index = (String) args[0];
                QueryConfig queryConfig = (QueryConfig) args[1];
                return new DataFrameAnalyticsSource(index, queryConfig);
            });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> QueryConfig.fromXContent(p), QUERY);
    }

    private final String index;
    private final QueryConfig queryConfig;

    public DataFrameAnalyticsSource(String index, @Nullable QueryConfig queryConfig) {
        this.index = Objects.requireNonNull(index);
        this.queryConfig = queryConfig;
    }

    public DataFrameAnalyticsSource(DataFrameAnalyticsSource other) {
        this(other.index, new QueryConfig(other.queryConfig));
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
        return Objects.equals(index, other.index)
            && Objects.equals(queryConfig, other.queryConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, queryConfig);
    }

    public String getIndex() {
        return index;
    }

    public QueryConfig getQueryConfig() {
        return queryConfig;
    }
}
