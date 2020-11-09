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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;


/**
 * PreProcessor for Shannon entropy calculation for a string
 */
public class Entropy implements PreProcessor {

    public static final String NAME = "entropy_encoding";
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField FEATURE_NAME = new ParseField("feature_name");
    public static final ParseField CUSTOM = new ParseField("custom");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Entropy, Void> PARSER = new ConstructingObjectParser<Entropy, Void>(
            NAME,
            true,
            a -> new Entropy((String)a[0],
                (String)a[1],
                (Boolean)a[2]));
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FEATURE_NAME);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), CUSTOM);
    }

    public static Entropy fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String field;
    private final String featureName;
    private final Boolean custom;

    Entropy(String field, String featureName, Boolean custom) {
        this.field = field;
        this.featureName = featureName;
        this.custom = custom;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field(FIELD.getPreferredName(), field);
        }
        if (featureName != null) {
            builder.field(FEATURE_NAME.getPreferredName(), featureName);
        }
        if (custom != null) {
            builder.field(CUSTOM.getPreferredName(), custom);
        }
        builder.endObject();
        return builder;
    }

    public String getField() {
        return field;
    }

    public String getFeatureName() {
        return featureName;
    }

    public Boolean getCustom() {
        return custom;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Entropy nGram = (Entropy) o;
        return Objects.equals(field, nGram.field) &&
            Objects.equals(featureName, nGram.featureName) &&
            Objects.equals(custom, nGram.custom);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, featureName, custom);
    }

    public static Builder builder(String field) {
        return new Builder(field);
    }

    public static class Builder {

        private String field;
        private String featureName;
        private Boolean custom;

        public Builder(String field) {
            this.field = field;
        }

        public Builder setField(String field) {
            this.field = field;
            return this;
        }

        public Builder setCustom(boolean custom) {
            this.custom = custom;
            return this;
        }

        public Builder setFeatureName(String featureName) {
            this.featureName = featureName;
            return this;
        }

        public Builder setCustom(Boolean custom) {
            this.custom = custom;
            return this;
        }

        public Entropy build() {
            return new Entropy(field, featureName, custom);
        }
    }
}
