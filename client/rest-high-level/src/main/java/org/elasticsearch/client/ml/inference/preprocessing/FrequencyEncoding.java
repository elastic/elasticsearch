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
 * PreProcessor for frequency encoding a set of categorical values for a given field.
 */
public class FrequencyEncoding implements PreProcessor {

    public static final String NAME = "frequency_encoding";
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField FEATURE_NAME = new ParseField("feature_name");
    public static final ParseField FREQUENCY_MAP = new ParseField("frequency_map");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<FrequencyEncoding, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new FrequencyEncoding((String)a[0], (String)a[1], (Map<String, Double>)a[2]));
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FEATURE_NAME);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(),
            (p, c) -> p.map(HashMap::new, XContentParser::doubleValue),
            FREQUENCY_MAP);
    }

    public static FrequencyEncoding fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String field;
    private final String featureName;
    private final Map<String, Double> frequencyMap;

    public FrequencyEncoding(String field, String featureName, Map<String, Double> frequencyMap) {
        this.field = Objects.requireNonNull(field);
        this.featureName = Objects.requireNonNull(featureName);
        this.frequencyMap = Collections.unmodifiableMap(Objects.requireNonNull(frequencyMap));
    }

    /**
     * @return Field name on which to frequency encode
     */
    public String getField() {
        return field;
    }

    /**
     * @return Map of Value: frequency for the frequency encoding
     */
    public Map<String, Double> getFrequencyMap() {
        return frequencyMap;
    }

    /**
     * @return The encoded feature name
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
        builder.field(FREQUENCY_MAP.getPreferredName(), frequencyMap);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FrequencyEncoding that = (FrequencyEncoding) o;
        return Objects.equals(field, that.field)
            && Objects.equals(featureName, that.featureName)
            && Objects.equals(frequencyMap, that.frequencyMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, featureName, frequencyMap);
    }

    public Builder builder(String field) {
        return new Builder(field);
    }

    public static class Builder {

        private String field;
        private String featureName;
        private Map<String, Double> frequencyMap = new HashMap<>();

        public Builder(String field) {
            this.field = field;
        }

        public Builder setField(String field) {
            this.field = field;
            return this;
        }

        public Builder setFeatureName(String featureName) {
            this.featureName = featureName;
            return this;
        }

        public Builder setFrequencyMap(Map<String, Double> frequencyMap) {
            this.frequencyMap = new HashMap<>(frequencyMap);
            return this;
        }

        public Builder addFrequency(String valueName, double frequency) {
            this.frequencyMap.put(valueName, frequency);
            return this;
        }

        public FrequencyEncoding build() {
            return new FrequencyEncoding(field, featureName, frequencyMap);
        }
    }

}
