/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.asyncsearch;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;
import java.util.Optional;

public class GetAsyncSearchRequest implements Validatable {

    private TimeValue waitForCompletion;
    private TimeValue keepAlive;

    private final String id;

    public GetAsyncSearchRequest(String id) {
        this.id = id;
    }

    public String getId() {
        return this.id;
    }

    public TimeValue getWaitForCompletion() {
        return waitForCompletion;
    }

    public void setWaitForCompletion(TimeValue waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
    }

    @Override
    public Optional<ValidationException> validate() {
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetAsyncSearchRequest request = (GetAsyncSearchRequest) o;
        return Objects.equals(getId(), request.getId())
            && Objects.equals(getKeepAlive(), request.getKeepAlive())
            && Objects.equals(getWaitForCompletion(), request.getWaitForCompletion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getKeepAlive(), getWaitForCompletion());
    }
}
