/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.inference.trainedmodel.ensemble;


import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;


public class LogisticRegression implements OutputAggregator {

    public static final String NAME = "logistic_regression";
    public static final ParseField WEIGHTS = new ParseField("weights");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<LogisticRegression, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new LogisticRegression((List<Double>)a[0]));
    static {
        PARSER.declareDoubleArray(ConstructingObjectParser.optionalConstructorArg(), WEIGHTS);
    }

    public static LogisticRegression fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final List<Double> weights;

    public LogisticRegression(List<Double> weights) {
        this.weights = weights;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (weights != null) {
            builder.field(WEIGHTS.getPreferredName(), weights);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogisticRegression that = (LogisticRegression) o;
        return Objects.equals(weights, that.weights);
    }

    @Override
    public int hashCode() {
        return Objects.hash(weights);
    }
}
