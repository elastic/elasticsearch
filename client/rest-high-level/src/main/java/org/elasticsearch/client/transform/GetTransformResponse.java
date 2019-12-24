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

package org.elasticsearch.client.transform;

import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class GetTransformResponse {

    public static final ParseField TRANSFORMS = new ParseField("transforms");
    public static final ParseField INVALID_TRANSFORMS = new ParseField("invalid_transforms");
    public static final ParseField COUNT = new ParseField("count");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<InvalidTransforms, Void> INVALID_TRANSFORMS_PARSER =
            new ConstructingObjectParser<>("invalid_transforms", true, args -> new InvalidTransforms((List<String>) args[0]));

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<GetTransformResponse, Void> PARSER = new ConstructingObjectParser<>(
            "get_transform", true, args -> new GetTransformResponse(
                    (List<TransformConfig>) args[0], (int) args[1], (InvalidTransforms) args[2]));
    static {
        // Discard the count field which is the size of the transforms array
        INVALID_TRANSFORMS_PARSER.declareInt((a, b) -> {}, COUNT);
        INVALID_TRANSFORMS_PARSER.declareStringArray(constructorArg(), TRANSFORMS);

        PARSER.declareObjectArray(constructorArg(), TransformConfig.PARSER::apply, TRANSFORMS);
        PARSER.declareInt(constructorArg(), COUNT);
        PARSER.declareObject(optionalConstructorArg(), INVALID_TRANSFORMS_PARSER::apply, INVALID_TRANSFORMS);
    }

    public static GetTransformResponse fromXContent(final XContentParser parser) {
        return GetTransformResponse.PARSER.apply(parser, null);
    }

    private List<TransformConfig> transformConfigurations;
    private int count;
    private InvalidTransforms invalidTransforms;

    public GetTransformResponse(List<TransformConfig> transformConfigurations,
                                int count,
                                @Nullable InvalidTransforms invalidTransforms) {
        this.transformConfigurations = transformConfigurations;
        this.count = count;
        this.invalidTransforms = invalidTransforms;
    }

    @Nullable
    public InvalidTransforms getInvalidTransforms() {
        return invalidTransforms;
    }

    public int getCount() {
        return count;
    }

    public List<TransformConfig> getTransformConfigurations() {
        return transformConfigurations;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transformConfigurations, count, invalidTransforms);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final GetTransformResponse that = (GetTransformResponse) other;
        return Objects.equals(this.transformConfigurations, that.transformConfigurations)
                && Objects.equals(this.count, that.count)
                && Objects.equals(this.invalidTransforms, that.invalidTransforms);
    }

    static class InvalidTransforms {
        private final List<String> transformIds;

        InvalidTransforms(List<String> transformIds) {
            this.transformIds = transformIds;
        }

        public int getCount() {
            return transformIds.size();
        }

        public List<String> getTransformIds() {
            return transformIds;
        }

        @Override
        public int hashCode() {
            return Objects.hash(transformIds);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            final InvalidTransforms that = (InvalidTransforms) other;
            return Objects.equals(this.transformIds, that.transformIds);
        }
    }
}
