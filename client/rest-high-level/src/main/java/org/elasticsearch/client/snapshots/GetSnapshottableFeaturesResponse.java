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

package org.elasticsearch.client.snapshots;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.List;
import java.util.Objects;

public class GetSnapshottableFeaturesResponse {

    private final List<SnapshottableFeature> features;

    private static final ParseField FEATURES = new ParseField("features");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GetSnapshottableFeaturesResponse, Void> PARSER = new ConstructingObjectParser<>(
        "snapshottable_features_response", true, (a, ctx) -> new GetSnapshottableFeaturesResponse((List<SnapshottableFeature>) a[0])
    );

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), SnapshottableFeature::parse, FEATURES);
    }

    public GetSnapshottableFeaturesResponse(List<SnapshottableFeature> features) {
        this.features = features;
    }

    public List<SnapshottableFeature> getFeatures() {
        return features;
    }

    public static GetSnapshottableFeaturesResponse parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GetSnapshottableFeaturesResponse)) return false;
        GetSnapshottableFeaturesResponse that = (GetSnapshottableFeaturesResponse) o;
        return getFeatures().equals(that.getFeatures());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFeatures());
    }

    public static class SnapshottableFeature {

        private final String featureName;
        private final String description;

        private static final ParseField FEATURE_NAME = new ParseField("name");
        private static final ParseField DESCRIPTION = new ParseField("description");

        private static final ConstructingObjectParser<SnapshottableFeature, Void> PARSER =  new ConstructingObjectParser<>(
            "feature", true, (a, ctx) -> new SnapshottableFeature((String) a[0], (String) a[1])
        );

        static {
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), FEATURE_NAME, ObjectParser.ValueType.STRING);
            PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.text(), DESCRIPTION, ObjectParser.ValueType.STRING);
        }

        public SnapshottableFeature(String featureName, String description) {
            this.featureName = featureName;
            this.description = description;
        }

        public static SnapshottableFeature parse(XContentParser parser, Void ctx) {
            return PARSER.apply(parser, ctx);
        }

        public String getFeatureName() {
            return featureName;
        }

        public String getDescription() {
            return description;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SnapshottableFeature)) return false;
            SnapshottableFeature feature = (SnapshottableFeature) o;
            return Objects.equals(getFeatureName(), feature.getFeatureName());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getFeatureName());
        }
    }
}
