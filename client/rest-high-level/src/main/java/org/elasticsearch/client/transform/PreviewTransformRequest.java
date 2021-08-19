/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class PreviewTransformRequest implements ToXContentObject, Validatable {

    private final String transformId;
    private final TransformConfig config;

    public PreviewTransformRequest(String transformId) {
        this.transformId = transformId;
        this.config = null;
    }

    public PreviewTransformRequest(TransformConfig config) {
        this.transformId = null;
        this.config = config;
    }

    public String getTransformId() {
        return transformId;
    }

    public TransformConfig getConfig() {
        return config;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (this.config != null) {
            return this.config.toXContent(builder, params);
        } else {
            return builder
                .startObject()
                .field(TransformConfig.ID.getPreferredName(), this.transformId)
                .endObject();
        }
    }

    @Override
    public Optional<ValidationException> validate() {
        ValidationException validationException = new ValidationException();
        if (config != null) {
            if (config.getSource() == null) {
                validationException.addValidationError("transform source cannot be null");
            }
        }
        if (transformId == null && config == null) {
            validationException.addValidationError("preview requires a non-null transform id or config");
            return Optional.of(validationException);
        }

        if (validationException.validationErrors().isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(validationException);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(transformId, config);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PreviewTransformRequest other = (PreviewTransformRequest) obj;
        return Objects.equals(transformId, other.transformId)
            && Objects.equals(config, other.config);
    }
}
