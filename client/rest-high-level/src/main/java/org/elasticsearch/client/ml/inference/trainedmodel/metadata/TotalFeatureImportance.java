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

package org.elasticsearch.client.ml.inference.trainedmodel.metadata;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TotalFeatureImportance implements ToXContentObject {

    private static final String NAME = "total_feature_importance";
    public static final ParseField FEATURE_NAME = new ParseField("feature_name");
    public static final ParseField IMPORTANCE = new ParseField("importance");
    public static final ParseField CLASSES = new ParseField("classes");
    public static final ParseField MEAN_MAGNITUDE = new ParseField("mean_magnitude");
    public static final ParseField BASELINE = new ParseField("baseline");
    public static final ParseField MIN = new ParseField("min");
    public static final ParseField MAX = new ParseField("max");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<TotalFeatureImportance, Void> PARSER = new ConstructingObjectParser<>(NAME,
        true,
        a -> new TotalFeatureImportance((String)a[0], (Importance)a[1], (List<ClassImportance>)a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FEATURE_NAME);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), Importance.PARSER, IMPORTANCE);
        PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), ClassImportance.PARSER, CLASSES);
    }

    public static TotalFeatureImportance fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public final String featureName;
    public final Importance importance;
    public final List<ClassImportance> classImportances;

    TotalFeatureImportance(String featureName, @Nullable Importance importance, @Nullable List<ClassImportance> classImportances) {
        this.featureName = featureName;
        this.importance = importance;
        this.classImportances = classImportances == null ? Collections.emptyList() : classImportances;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FEATURE_NAME.getPreferredName(), featureName);
        if (importance != null) {
            builder.field(IMPORTANCE.getPreferredName(), importance);
        }
        if (classImportances.isEmpty() == false) {
            builder.field(CLASSES.getPreferredName(), classImportances);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TotalFeatureImportance that = (TotalFeatureImportance) o;
        return Objects.equals(that.importance, importance)
            && Objects.equals(featureName, that.featureName)
            && Objects.equals(classImportances, that.classImportances);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureName, importance, classImportances);
    }

    public static class Importance implements ToXContentObject {
        private static final String NAME = "importance";

        public static final ConstructingObjectParser<Importance, Void> PARSER = new ConstructingObjectParser<>(NAME,
            true,
            a -> new Importance((double)a[0], (double)a[1], (double)a[2], (Double)a[3]));

        static {
            PARSER.declareDouble(ConstructingObjectParser.constructorArg(), MEAN_MAGNITUDE);
            PARSER.declareDouble(ConstructingObjectParser.constructorArg(), MIN);
            PARSER.declareDouble(ConstructingObjectParser.constructorArg(), MAX);
            PARSER.declareDouble(ConstructingObjectParser.optionalConstructorArg(), BASELINE);
        }

        private final double meanMagnitude;
        private final double min;
        private final double max;
        private final Double baseline;

        public Importance(double meanMagnitude, double min, double max, Double baseline) {
            this.meanMagnitude = meanMagnitude;
            this.min = min;
            this.max = max;
            this.baseline = baseline;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Importance that = (Importance) o;
            return Double.compare(that.meanMagnitude, meanMagnitude) == 0 &&
                Double.compare(that.min, min) == 0 &&
                Double.compare(that.max, max) == 0 &&
                Objects.equals(that.baseline, baseline);
        }

        @Override
        public int hashCode() {
            return Objects.hash(meanMagnitude, min, max, baseline);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MEAN_MAGNITUDE.getPreferredName(), meanMagnitude);
            builder.field(MIN.getPreferredName(), min);
            builder.field(MAX.getPreferredName(), max);
            if (baseline != null) {
                builder.field(BASELINE.getPreferredName(), baseline);
            }
            builder.endObject();
            return builder;
        }
    }

    public static class ClassImportance implements ToXContentObject {
        private static final String NAME = "total_class_importance";

        public static final ParseField CLASS_NAME = new ParseField("class_name");
        public static final ParseField IMPORTANCE = new ParseField("importance");

        public static final ConstructingObjectParser<ClassImportance, Void> PARSER = new ConstructingObjectParser<>(NAME,
            true,
            a -> new ClassImportance(a[0], (Importance)a[1]));

        static {
            PARSER.declareField(ConstructingObjectParser.constructorArg(), (p, c) -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return p.text();
                } else if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                    return p.numberValue();
                } else if (p.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
                    return p.booleanValue();
                }
                throw new XContentParseException("Unsupported token [" + p.currentToken() + "]");
            }, CLASS_NAME, ObjectParser.ValueType.VALUE);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), Importance.PARSER, IMPORTANCE);
        }

        public static ClassImportance fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        public final Object className;
        public final Importance importance;

        ClassImportance(Object className, Importance importance) {
            this.className = className;
            this.importance = importance;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CLASS_NAME.getPreferredName(), className);
            builder.field(IMPORTANCE.getPreferredName(), importance);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClassImportance that = (ClassImportance) o;
            return Objects.equals(that.importance, importance) && Objects.equals(className, that.className);
        }

        @Override
        public int hashCode() {
            return Objects.hash(className, importance);
        }

    }
}
