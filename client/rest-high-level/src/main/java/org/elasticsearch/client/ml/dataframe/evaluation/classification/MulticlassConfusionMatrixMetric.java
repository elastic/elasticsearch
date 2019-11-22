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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
        private static final ParseField OTHER_ACTUAL_CLASS_COUNT = new ParseField("other_actual_class_count");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Result, Void> PARSER =
            new ConstructingObjectParser<>(
                "multiclass_confusion_matrix_result", true, a -> new Result((List<ActualClass>) a[0], (Long) a[1]));

        static {
            PARSER.declareObjectArray(optionalConstructorArg(), ActualClass.PARSER, CONFUSION_MATRIX);
            PARSER.declareLong(optionalConstructorArg(), OTHER_ACTUAL_CLASS_COUNT);
        }

        public static Result fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private final List<ActualClass> confusionMatrix;
        private final Long otherActualClassCount;

        public Result(@Nullable List<ActualClass> confusionMatrix, @Nullable Long otherActualClassCount) {
            this.confusionMatrix = confusionMatrix != null ? Collections.unmodifiableList(Objects.requireNonNull(confusionMatrix)) : null;
            this.otherActualClassCount = otherActualClassCount;
        }

        @Override
        public String getMetricName() {
            return NAME;
        }

        public List<ActualClass> getConfusionMatrix() {
            return confusionMatrix;
        }

        public Long getOtherActualClassCount() {
            return otherActualClassCount;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (confusionMatrix != null) {
                builder.field(CONFUSION_MATRIX.getPreferredName(), confusionMatrix);
            }
            if (otherActualClassCount != null) {
                builder.field(OTHER_ACTUAL_CLASS_COUNT.getPreferredName(), otherActualClassCount);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return Objects.equals(this.confusionMatrix, that.confusionMatrix)
                && Objects.equals(this.otherActualClassCount, that.otherActualClassCount);
        }

        @Override
        public int hashCode() {
            return Objects.hash(confusionMatrix, otherActualClassCount);
        }
    }

    public static class ActualClass implements ToXContentObject {

        private static final ParseField ACTUAL_CLASS = new ParseField("actual_class");
        private static final ParseField ACTUAL_CLASS_DOC_COUNT = new ParseField("actual_class_doc_count");
        private static final ParseField PREDICTED_CLASSES = new ParseField("predicted_classes");
        private static final ParseField OTHER_PREDICTED_CLASS_DOC_COUNT = new ParseField("other_predicted_class_doc_count");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<ActualClass, Void> PARSER =
            new ConstructingObjectParser<>(
                "multiclass_confusion_matrix_actual_class",
                true,
                a -> new ActualClass((String) a[0], (Long) a[1], (List<PredictedClass>) a[2], (Long) a[3]));

        static {
            PARSER.declareString(optionalConstructorArg(), ACTUAL_CLASS);
            PARSER.declareLong(optionalConstructorArg(), ACTUAL_CLASS_DOC_COUNT);
            PARSER.declareObjectArray(optionalConstructorArg(), PredictedClass.PARSER, PREDICTED_CLASSES);
            PARSER.declareLong(optionalConstructorArg(), OTHER_PREDICTED_CLASS_DOC_COUNT);
        }

        private final String actualClass;
        private final Long actualClassDocCount;
        private final List<PredictedClass> predictedClasses;
        private final Long otherPredictedClassDocCount;

        public ActualClass(@Nullable String actualClass,
                           @Nullable Long actualClassDocCount,
                           @Nullable List<PredictedClass> predictedClasses,
                           @Nullable Long otherPredictedClassDocCount) {
            this.actualClass = actualClass;
            this.actualClassDocCount = actualClassDocCount;
            this.predictedClasses = predictedClasses != null ? Collections.unmodifiableList(predictedClasses) : null;
            this.otherPredictedClassDocCount = otherPredictedClassDocCount;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (actualClass != null) {
                builder.field(ACTUAL_CLASS.getPreferredName(), actualClass);
            }
            if (actualClassDocCount != null) {
                builder.field(ACTUAL_CLASS_DOC_COUNT.getPreferredName(), actualClassDocCount);
            }
            if (predictedClasses != null) {
                builder.field(PREDICTED_CLASSES.getPreferredName(), predictedClasses);
            }
            if (otherPredictedClassDocCount != null) {
                builder.field(OTHER_PREDICTED_CLASS_DOC_COUNT.getPreferredName(), otherPredictedClassDocCount);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ActualClass that = (ActualClass) o;
            return Objects.equals(this.actualClass, that.actualClass)
                && Objects.equals(this.actualClassDocCount, that.actualClassDocCount)
                && Objects.equals(this.predictedClasses, that.predictedClasses)
                && Objects.equals(this.otherPredictedClassDocCount, that.otherPredictedClassDocCount);
        }

        @Override
        public int hashCode() {
            return Objects.hash(actualClass, actualClassDocCount, predictedClasses, otherPredictedClassDocCount);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static class PredictedClass implements ToXContentObject {

        private static final ParseField PREDICTED_CLASS = new ParseField("predicted_class");
        private static final ParseField COUNT = new ParseField("count");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<PredictedClass, Void> PARSER =
            new ConstructingObjectParser<>(
                "multiclass_confusion_matrix_predicted_class", true, a -> new PredictedClass((String) a[0], (Long) a[1]));

        static {
            PARSER.declareString(optionalConstructorArg(), PREDICTED_CLASS);
            PARSER.declareLong(optionalConstructorArg(), COUNT);
        }

        private final String predictedClass;
        private final Long count;

        public PredictedClass(@Nullable String predictedClass, @Nullable Long count) {
            this.predictedClass = predictedClass;
            this.count = count;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (predictedClass != null) {
                builder.field(PREDICTED_CLASS.getPreferredName(), predictedClass);
            }
            if (count != null) {
                builder.field(COUNT.getPreferredName(), count);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PredictedClass that = (PredictedClass) o;
            return Objects.equals(this.predictedClass, that.predictedClass)
                && Objects.equals(this.count, that.count);
        }

        @Override
        public int hashCode() {
            return Objects.hash(predictedClass, count);
        }
    }
}
