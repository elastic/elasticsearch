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
package org.elasticsearch.client.ml.dataframe.evaluation.classification;

import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Calculates the multiclass confusion matrix.
 */
public class MulticlassConfusionMatrixMetric implements EvaluationMetric {

    public static final String NAME = "multiclass_confusion_matrix";

    public static final ParseField SIZE = new ParseField("size");

    private static final ConstructingObjectParser<MulticlassConfusionMatrixMetric, Void> PARSER = createParser();

    private static ConstructingObjectParser<MulticlassConfusionMatrixMetric, Void> createParser() {
        ConstructingObjectParser<MulticlassConfusionMatrixMetric, Void>  parser =
            new ConstructingObjectParser<>(NAME, true, args -> new MulticlassConfusionMatrixMetric((Integer) args[0]));
        parser.declareInt(optionalConstructorArg(), SIZE);
        return parser;
    }

    public static MulticlassConfusionMatrixMetric fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final Integer size;

    public MulticlassConfusionMatrixMetric() {
        this(null);
    }

    public MulticlassConfusionMatrixMetric(@Nullable Integer size) {
        this.size = size;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (size != null) {
            builder.field(SIZE.getPreferredName(), size);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MulticlassConfusionMatrixMetric that = (MulticlassConfusionMatrixMetric) o;
        return Objects.equals(this.size, that.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(size);
    }

    public static class Result implements EvaluationMetric.Result {

        private static final ParseField CONFUSION_MATRIX = new ParseField("confusion_matrix");
        private static final ParseField OTHER_CLASSES_COUNT = new ParseField("_other_");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Result, Void> PARSER =
            new ConstructingObjectParser<>(
                "multiclass_confusion_matrix_result", true, a -> new Result((Map<String, Map<String, Long>>) a[0], (long) a[1]));

        static {
            PARSER.declareObject(
                constructorArg(),
                (p, c) -> p.map(TreeMap::new, p2 -> p2.map(TreeMap::new, XContentParser::longValue)),
                CONFUSION_MATRIX);
            PARSER.declareLong(constructorArg(), OTHER_CLASSES_COUNT);
        }

        public static Result fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        // Immutable
        private final Map<String, Map<String, Long>> confusionMatrix;
        private final long otherClassesCount;

        public Result(Map<String, Map<String, Long>> confusionMatrix, long otherClassesCount) {
            this.confusionMatrix = Collections.unmodifiableMap(Objects.requireNonNull(confusionMatrix));
            this.otherClassesCount = otherClassesCount;
        }

        @Override
        public String getMetricName() {
            return NAME;
        }

        public Map<String, Map<String, Long>> getConfusionMatrix() {
            return confusionMatrix;
        }

        public long getOtherClassesCount() {
            return otherClassesCount;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CONFUSION_MATRIX.getPreferredName(), confusionMatrix);
            builder.field(OTHER_CLASSES_COUNT.getPreferredName(), otherClassesCount);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return Objects.equals(this.confusionMatrix, that.confusionMatrix)
                && this.otherClassesCount == that.otherClassesCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(confusionMatrix, otherClassesCount);
        }
    }
}
