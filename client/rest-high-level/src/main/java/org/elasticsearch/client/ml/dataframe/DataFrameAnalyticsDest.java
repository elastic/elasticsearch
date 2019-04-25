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
import org.elasticsearch.common.inject.internal.ToStringBuilder;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class DataFrameAnalyticsDest implements ToXContentObject {

    public static DataFrameAnalyticsDest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static final ParseField INDEX = new ParseField("index");
    private static final ParseField RESULTS_FIELD = new ParseField("results_field");

    private static ConstructingObjectParser<DataFrameAnalyticsDest, Void> PARSER =
        new ConstructingObjectParser<>("data_frame_analytics_dest", true,
            (args) -> {
                String index = (String) args[0];
                String resultsField = (String) args[1];
                return new DataFrameAnalyticsDest(index, resultsField);
            });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), RESULTS_FIELD);
    }

    private final String index;
    private final String resultsField;

    public DataFrameAnalyticsDest(String index) {
        this(index, null);
    }

    public DataFrameAnalyticsDest(String index, @Nullable String resultsField) {
        this.index = requireNonNull(index);
        this.resultsField = resultsField;
    }

    public DataFrameAnalyticsDest(DataFrameAnalyticsDest other) {
        this(other.index, other.resultsField);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX.getPreferredName(), index);
        if (resultsField != null) {
            builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataFrameAnalyticsDest other = (DataFrameAnalyticsDest) o;
        return Objects.equals(index, other.index)
            && Objects.equals(resultsField, other.resultsField);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(getClass())
            .add("index", index)
            .add("resultsField", resultsField)
            .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, resultsField);
    }

    public String getIndex() {
        return index;
    }

    public String getResultsField() {
        return resultsField;
    }
}
