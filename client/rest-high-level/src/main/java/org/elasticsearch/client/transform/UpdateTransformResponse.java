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
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Objects;

public class UpdateTransformResponse {

    public static UpdateTransformResponse fromXContent(final XContentParser parser) {
        return new UpdateTransformResponse(TransformConfig.PARSER.apply(parser, null));
    }

    private TransformConfig transformConfiguration;

    public UpdateTransformResponse(TransformConfig transformConfiguration) {
        this.transformConfiguration = transformConfiguration;
    }

    public TransformConfig getTransformConfiguration() {
        return transformConfiguration;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transformConfiguration);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final UpdateTransformResponse that = (UpdateTransformResponse) other;
        return Objects.equals(this.transformConfiguration, that.transformConfiguration);
    }
}
