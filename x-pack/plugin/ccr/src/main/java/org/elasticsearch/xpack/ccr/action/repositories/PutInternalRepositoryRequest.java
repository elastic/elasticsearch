/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.repositories;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Objects;

public class PutInternalRepositoryRequest extends ActionRequest {

    private final String name;
    private final String type;
    private final Settings settings;

    public PutInternalRepositoryRequest(String name, String type) {
        this(name, type, Settings.EMPTY);
    }

    public PutInternalRepositoryRequest(String name, String type, Settings settings) {
        this.name = Objects.requireNonNull(name);
        this.type = Objects.requireNonNull(type);
        this.settings = settings;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("PutInternalRepositoryRequest cannot be serialized for sending across the wire.");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("PutInternalRepositoryRequest cannot be serialized for sending across the wire.");
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public Settings getSettings() {
        return settings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutInternalRepositoryRequest that = (PutInternalRepositoryRequest) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(type, that.type) &&
            Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, settings);
    }

    @Override
    public String toString() {
        return "PutInternalRepositoryRequest{" +
            "name='" + name + '\'' +
            ", type='" + type + '\'' +
            ", settings=" + settings +
            '}';
    }
}
