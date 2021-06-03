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
import org.elasticsearch.client.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class UpdateTransformRequest implements ToXContentObject, Validatable {

    private final TransformConfigUpdate update;
    private final String id;
    private Boolean deferValidation;

    public UpdateTransformRequest(TransformConfigUpdate update, String id) {
        this.update = update;
        this.id = id;
    }

    public TransformConfigUpdate getUpdate() {
        return update;
    }

    public Boolean getDeferValidation() {
        return deferValidation;
    }

    public String getId() {
        return id;
    }

    /**
     * Indicates if deferrable validations should be skipped until the transform starts
     *
     * @param deferValidation {@code true} will cause validations to be deferred
     */
    public void setDeferValidation(boolean deferValidation) {
        this.deferValidation = deferValidation;
    }

    @Override
    public Optional<ValidationException> validate() {
        ValidationException validationException = new ValidationException();
        if (update == null) {
            validationException.addValidationError("put requires a non-null transform config update object");
        }
        if (id == null) {
            validationException.addValidationError("transform id cannot be null");
        }
        if (validationException.validationErrors().isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(validationException);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return update.toXContent(builder, params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(update, deferValidation, id);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        UpdateTransformRequest other = (UpdateTransformRequest) obj;
        return Objects.equals(update, other.update)
            && Objects.equals(id, other.id)
            && Objects.equals(deferValidation, other.deferValidation);
    }
}
