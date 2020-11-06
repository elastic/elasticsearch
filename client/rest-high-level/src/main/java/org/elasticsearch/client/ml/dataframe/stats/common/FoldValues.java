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
package org.elasticsearch.client.ml.dataframe.stats.common;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class FoldValues implements ToXContentObject {

    public static final ParseField FOLD = new ParseField("fold");
    public static final ParseField VALUES = new ParseField("values");

    @SuppressWarnings("unchecked")
    public static ConstructingObjectParser<FoldValues, Void> PARSER = new ConstructingObjectParser<>("fold_values", true,
        a -> new FoldValues((int) a[0], (List<Double>) a[1]));

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), FOLD);
        PARSER.declareDoubleArray(ConstructingObjectParser.constructorArg(), VALUES);
    }

    private final int fold;
    private final double[] values;

    private FoldValues(int fold, List<Double> values) {
        this(fold, values.stream().mapToDouble(Double::doubleValue).toArray());
    }

    public FoldValues(int fold, double[] values) {
        this.fold = fold;
        this.values = values;
    }

    public int getFold() {
        return fold;
    }

    public double[] getValues() {
        return values;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FOLD.getPreferredName(), fold);
        builder.array(VALUES.getPreferredName(), values);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FoldValues other = (FoldValues) o;
        return fold == other.fold && Arrays.equals(values, other.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fold, Arrays.hashCode(values));
    }
}
