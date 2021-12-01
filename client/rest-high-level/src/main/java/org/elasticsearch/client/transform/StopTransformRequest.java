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
import org.elasticsearch.core.TimeValue;

import java.util.Objects;
import java.util.Optional;

public class StopTransformRequest implements Validatable {

    public static final String WAIT_FOR_CHECKPOINT = "wait_for_checkpoint";

    private final String id;
    private Boolean waitForCompletion;
    private Boolean waitForCheckpoint;
    private TimeValue timeout;
    private Boolean allowNoMatch;

    public StopTransformRequest(String id) {
        this(id, null, null, null);
    }

    public StopTransformRequest(String id, Boolean waitForCompletion, TimeValue timeout, Boolean waitForCheckpoint) {
        this.id = id;
        this.waitForCompletion = waitForCompletion;
        this.timeout = timeout;
        this.waitForCheckpoint = waitForCheckpoint;
    }

    public String getId() {
        return id;
    }

    public void setWaitForCompletion(Boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    public Boolean getWaitForCompletion() {
        return waitForCompletion;
    }

    public void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public Boolean getAllowNoMatch() {
        return allowNoMatch;
    }

    public void setAllowNoMatch(Boolean allowNoMatch) {
        this.allowNoMatch = allowNoMatch;
    }

    public Boolean getWaitForCheckpoint() {
        return waitForCheckpoint;
    }

    public void setWaitForCheckpoint(Boolean waitForCheckpoint) {
        this.waitForCheckpoint = waitForCheckpoint;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (id == null) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationError("transform id must not be null");
            return Optional.of(validationException);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, waitForCompletion, timeout, allowNoMatch, waitForCheckpoint);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        StopTransformRequest other = (StopTransformRequest) obj;
        return Objects.equals(this.id, other.id)
            && Objects.equals(this.waitForCompletion, other.waitForCompletion)
            && Objects.equals(this.timeout, other.timeout)
            && Objects.equals(this.waitForCheckpoint, other.waitForCheckpoint)
            && Objects.equals(this.allowNoMatch, other.allowNoMatch);
    }

}
