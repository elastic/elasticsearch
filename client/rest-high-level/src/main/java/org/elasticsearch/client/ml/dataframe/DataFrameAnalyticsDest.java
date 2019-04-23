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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class DataFrameAnalyticsDest implements ToXContentObject {

    public static DataFrameAnalyticsDest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static final ParseField INDEX = new ParseField("index");

    private static ConstructingObjectParser<DataFrameAnalyticsDest, Void> PARSER =
        new ConstructingObjectParser<>("data_frame_analytics_dest", true,
            (args) -> {
                String index = (String) args[0];
                return new DataFrameAnalyticsDest(index);
            });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX);
    }

    private final String index;

    public DataFrameAnalyticsDest(String index) {
        this.index = Objects.requireNonNull(index);
    }

    public DataFrameAnalyticsDest(DataFrameAnalyticsDest other) {
        this.index = other.index;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX.getPreferredName(), index);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataFrameAnalyticsDest other = (DataFrameAnalyticsDest) o;
        return Objects.equals(index, other.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }

    public String getIndex() {
        return index;
    }
}
