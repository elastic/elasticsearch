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
package org.elasticsearch.client.ml.dataframe.evaluation.common;

import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class AucRocResult implements EvaluationMetric.Result {

    public static final String NAME = "auc_roc";

    public static AucRocResult fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static final ParseField VALUE = new ParseField("value");
    private static final ParseField CURVE = new ParseField("curve");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<AucRocResult, Void> PARSER =
        new ConstructingObjectParser<>(
            NAME, true, args -> new AucRocResult((double) args[0], (List<AucRocPoint>) args[1]));

    static {
        PARSER.declareDouble(constructorArg(), VALUE);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> AucRocPoint.fromXContent(p), CURVE);
    }

    private final double value;
    private final List<AucRocPoint> curve;

    public AucRocResult(double value, @Nullable List<AucRocPoint> curve) {
        this.value = value;
        this.curve = curve;
    }

    @Override
    public String getMetricName() {
        return NAME;
    }

    public double getValue() {
        return value;
    }

    public List<AucRocPoint> getCurve() {
        return curve == null ? null : Collections.unmodifiableList(curve);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(VALUE.getPreferredName(), value);
        if (curve != null && curve.isEmpty() == false) {
            builder.field(CURVE.getPreferredName(), curve);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AucRocResult that = (AucRocResult) o;
        return value == that.value
            && Objects.equals(curve, that.curve);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, curve);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
