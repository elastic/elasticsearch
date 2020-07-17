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
 * PreProcessor for one hot encoding a set of categorical values for a given field.
 */
public class OneHotEncoding implements PreProcessor {

    public static final String NAME = "one_hot_encoding";
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField HOT_MAP = new ParseField("hot_map");
    public static final ParseField CUSTOM = new ParseField("custom");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<OneHotEncoding, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new OneHotEncoding((String)a[0], (Map<String, String>)a[1], (Boolean)a[2]));
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), HOT_MAP);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), CUSTOM);
    }

    public static OneHotEncoding fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String field;
    private final Map<String, String> hotMap;
    private final Boolean custom;

    OneHotEncoding(String field, Map<String, String> hotMap, Boolean custom) {
        this.field = Objects.requireNonNull(field);
        this.hotMap = Collections.unmodifiableMap(Objects.requireNonNull(hotMap));
        this.custom = custom;
    }
    /**
     * @return Field name on which to one hot encode
     */
    public String getField() {
        return field;
    }

    /**
     * @return Map of Value: ColumnName for the one hot encoding
     */
    public Map<String, String> getHotMap() {
        return hotMap;
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
        builder.field(HOT_MAP.getPreferredName(), hotMap);
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
        OneHotEncoding that = (OneHotEncoding) o;
        return Objects.equals(field, that.field)
            && Objects.equals(hotMap, that.hotMap)
            && Objects.equals(custom, that.custom);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, hotMap, custom);
    }

    public Builder builder(String field) {
        return new Builder(field);
    }

    public static class Builder {

        private String field;
        private Map<String, String> hotMap = new HashMap<>();
        private Boolean custom;

        public Builder(String field) {
            this.field = field;
        }

        public Builder setField(String field) {
            this.field = field;
            return this;
        }

        public Builder setHotMap(Map<String, String> hotMap) {
            this.hotMap = new HashMap<>(hotMap);
            return this;
        }

        public Builder addOneHot(String valueName, String oneHotFeatureName) {
            this.hotMap.put(valueName, oneHotFeatureName);
            return this;
        }

        public Builder setCustom(boolean custom) {
            this.custom = custom;
            return this;
        }

        public OneHotEncoding build() {
            return new OneHotEncoding(field, hotMap, custom);
        }
    }
}
