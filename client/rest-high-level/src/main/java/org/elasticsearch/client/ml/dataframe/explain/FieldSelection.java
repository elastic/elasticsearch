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
package org.elasticsearch.client.ml.dataframe.explain;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public class FieldSelection implements ToXContentObject {

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField MAPPING_TYPES = new ParseField("mapping_types");
    private static final ParseField IS_INCLUDED = new ParseField("is_included");
    private static final ParseField IS_REQUIRED = new ParseField("is_required");
    private static final ParseField FEATURE_TYPE = new ParseField("feature_type");
    private static final ParseField REASON = new ParseField("reason");

    public enum FeatureType {
        CATEGORICAL, NUMERICAL;

        public static FeatureType fromString(String value) {
            return FeatureType.valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<FieldSelection, Void> PARSER = new ConstructingObjectParser<>("field_selection", true,
        a -> new FieldSelection((String) a[0], new HashSet<>((List<String>) a[1]), (boolean) a[2], (boolean) a[3], (FeatureType) a[4],
            (String) a[5]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), MAPPING_TYPES);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), IS_INCLUDED);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), IS_REQUIRED);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return FeatureType.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, FEATURE_TYPE, ObjectParser.ValueType.STRING);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), REASON);
    }

    private final String name;
    private final Set<String> mappingTypes;
    private final boolean isIncluded;
    private final boolean isRequired;
    private final FeatureType featureType;
    private final String reason;

    public static FieldSelection included(String name, Set<String> mappingTypes, boolean isRequired, FeatureType featureType) {
        return new FieldSelection(name, mappingTypes, true, isRequired, featureType, null);
    }

    public static FieldSelection excluded(String name, Set<String> mappingTypes, String reason) {
        return new FieldSelection(name, mappingTypes, false, false, null, reason);
    }

    FieldSelection(String name, Set<String> mappingTypes, boolean isIncluded, boolean isRequired, @Nullable FeatureType featureType,
                           @Nullable String reason) {
        this.name = Objects.requireNonNull(name);
        this.mappingTypes = Collections.unmodifiableSet(mappingTypes);
        this.isIncluded = isIncluded;
        this.isRequired = isRequired;
        this.featureType = featureType;
        this.reason = reason;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME.getPreferredName(), name);
        builder.field(MAPPING_TYPES.getPreferredName(), mappingTypes);
        builder.field(IS_INCLUDED.getPreferredName(), isIncluded);
        builder.field(IS_REQUIRED.getPreferredName(), isRequired);
        if (featureType != null) {
            builder.field(FEATURE_TYPE.getPreferredName(), featureType);
        }
        if (reason != null) {
            builder.field(REASON.getPreferredName(), reason);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldSelection that = (FieldSelection) o;
        return Objects.equals(name, that.name)
            && Objects.equals(mappingTypes, that.mappingTypes)
            && isIncluded == that.isIncluded
            && isRequired == that.isRequired
            && Objects.equals(featureType, that.featureType)
            && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, mappingTypes, isIncluded, isRequired, featureType, reason);
    }

    public String getName() {
        return name;
    }

    public Set<String> getMappingTypes() {
        return mappingTypes;
    }

    public boolean isIncluded() {
        return isIncluded;
    }

    public boolean isRequired() {
        return isRequired;
    }

    @Nullable
    public FeatureType getFeatureType() {
        return featureType;
    }

    @Nullable
    public String getReason() {
        return reason;
    }
}
