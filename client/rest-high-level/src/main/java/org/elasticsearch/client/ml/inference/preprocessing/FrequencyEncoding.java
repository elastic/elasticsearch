/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference.preprocessing;

import org.elasticsearch.common.xcontent.ParseField;
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
    public static final ParseField CUSTOM = new ParseField("custom");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<FrequencyEncoding, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new FrequencyEncoding((String)a[0], (String)a[1], (Map<String, Double>)a[2], (Boolean)a[3]));
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FEATURE_NAME);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(),
            (p, c) -> p.map(HashMap::new, XContentParser::doubleValue),
            FREQUENCY_MAP);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), CUSTOM);
    }

    public static FrequencyEncoding fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String field;
    private final String featureName;
    private final Map<String, Double> frequencyMap;
    private final Boolean custom;

    FrequencyEncoding(String field, String featureName, Map<String, Double> frequencyMap, Boolean custom) {
        this.field = Objects.requireNonNull(field);
        this.featureName = Objects.requireNonNull(featureName);
        this.frequencyMap = Collections.unmodifiableMap(Objects.requireNonNull(frequencyMap));
        this.custom = custom;
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

    public Boolean getCustom() {
        return custom;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD.getPreferredName(), field);
        builder.field(FEATURE_NAME.getPreferredName(), featureName);
        builder.field(FREQUENCY_MAP.getPreferredName(), frequencyMap);
        if (custom != null) {
            builder.field(CUSTOM.getPreferredName(), custom);
        }
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
            && Objects.equals(custom, that.custom)
            && Objects.equals(frequencyMap, that.frequencyMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, featureName, frequencyMap, custom);
    }

    public Builder builder(String field) {
        return new Builder(field);
    }

    public static class Builder {

        private String field;
        private String featureName;
        private Map<String, Double> frequencyMap = new HashMap<>();
        private Boolean custom;

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

        public Builder setCustom(boolean custom) {
            this.custom = custom;
            return this;
        }

        public FrequencyEncoding build() {
            return new FrequencyEncoding(field, featureName, frequencyMap, custom);
        }
    }

}
