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
package org.elasticsearch.client.ml.dataframe.evaluation.softclassification;

import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

abstract class AbstractConfusionMatrixMetric implements EvaluationMetric {

    protected static final ParseField AT = new ParseField("at");

    protected final double[] thresholds;

    protected AbstractConfusionMatrixMetric(List<Double> at) {
        this(at.stream().mapToDouble(Double::doubleValue).toArray());
    }

    private AbstractConfusionMatrixMetric(double[] thresholds) {
        this.thresholds = Objects.requireNonNull(thresholds);
        if (thresholds.length == 0) {
            throw new IllegalArgumentException("[" + getName() + "." + AT.getPreferredName() + "] must have at least one value");
        }
        for (double threshold : thresholds) {
            if (threshold < 0 || threshold > 1.0) {
                throw new IllegalArgumentException("[" + getName() + "." + AT.getPreferredName() + "] values must be in [0.0, 1.0]");
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        return builder
            .startObject()
            .field(AT.getPreferredName(), thresholds)
            .endObject();
    }
}
