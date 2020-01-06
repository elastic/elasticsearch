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
package org.elasticsearch.client.ml.inference.preprocessing;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


/**
 * PreProcessor for target mean encoding a set of categorical values for a given field.
 */
public class TargetMeanEncoding implements PreProcessor {

    public static final String NAME = "target_mean_encoding";
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField FEATURE_NAME = new ParseField("feature_name");
    public static final ParseField TARGET_MAP = new ParseField("target_map");
    public static final ParseField DEFAULT_VALUE = new ParseField("default_value");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<TargetMeanEncoding, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new TargetMeanEncoding((String)a[0], (String)a[1], (Map<String, Double>)a[2], (Double)a[3]));
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FEATURE_NAME);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(),
            (p, c) -> p.map(HashMap::new, XContentParser::doubleValue),
            TARGET_MAP);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), DEFAULT_VALUE);
    }

    public static TargetMeanEncoding fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String field;
    private final String featureName;
    private final Map<String, Double> meanMap;
    private final double defaultValue;

    public TargetMeanEncoding(String field, String featureName, Map<String, Double> meanMap, Double defaultValue) {
        this.field = Objects.requireNonNull(field);
        this.featureName = Objects.requireNonNull(featureName);
        this.meanMap = Collections.unmodifiableMap(Objects.requireNonNull(meanMap));
        this.defaultValue = Objects.requireNonNull(defaultValue);
    }

    /**
     * @return Field name on which to target mean encode
     */
    public String getField() {
        return field;
    }

    /**
     * @return Map of Value: targetMean for the target mean encoding
     */
    public Map<String, Double> getMeanMap() {
        return meanMap;
    }

    /**
     * @return The default value to set when a previously unobserved value is seen
     */
    public double getDefaultValue() {
        return defaultValue;
    }

    /**
     * @return The feature name for the encoded value
     */
    public String getFeatureName() {
        return featureName;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD.getPreferredName(), field);
        builder.field(FEATURE_NAME.getPreferredName(), featureName);
        builder.field(TARGET_MAP.getPreferredName(), meanMap);
        builder.field(DEFAULT_VALUE.getPreferredName(), defaultValue);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TargetMeanEncoding that = (TargetMeanEncoding) o;
        return Objects.equals(field, that.field)
            && Objects.equals(featureName, that.featureName)
            && Objects.equals(meanMap, that.meanMap)
            && Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, featureName, meanMap, defaultValue);
    }

    public Builder builder(String field) {
        return new Builder(field);
    }

    public static class Builder {

        private String field;
        private String featureName;
        private Map<String, Double> meanMap = new HashMap<>();
        private double defaultValue;

        public Builder(String field) {
            this.field = field;
        }

        public String getField() {
            return field;
        }

        public Builder setField(String field) {
            this.field = field;
            return this;
        }

        public Builder setFeatureName(String featureName) {
            this.featureName = featureName;
            return this;
        }

        public Builder setMeanMap(Map<String, Double> meanMap) {
            this.meanMap = meanMap;
            return this;
        }

        public Builder addMeanMapEntry(String valueName, double meanEncoding) {
            this.meanMap.put(valueName, meanEncoding);
            return this;
        }

        public Builder setDefaultValue(double defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public TargetMeanEncoding build() {
            return new TargetMeanEncoding(field, featureName, meanMap, defaultValue);
        }
    }
}
