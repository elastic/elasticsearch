/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform;

import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.xcontent.XContentParser;

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
