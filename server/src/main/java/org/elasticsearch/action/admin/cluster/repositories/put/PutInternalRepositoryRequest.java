/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.repositories.put;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutInternalRepositoryRequest extends ActionRequest {

    private String name;
    private String type;
    private Settings settings;

    public PutInternalRepositoryRequest(String name, String type) {
        this(name, type, Settings.EMPTY);
    }

    public PutInternalRepositoryRequest(String name, String type, Settings settings) {
        this.name = name;
        this.type = type;
        this.settings = settings;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        if (type == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
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
