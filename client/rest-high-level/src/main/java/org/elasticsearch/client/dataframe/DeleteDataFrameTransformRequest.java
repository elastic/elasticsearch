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

package org.elasticsearch.client.dataframe;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;

import java.util.Objects;
import java.util.Optional;


/**
 * Request to delete a data frame transform
 */
public class DeleteDataFrameTransformRequest implements Validatable {

    public static final String FORCE = "force";

    private final String id;
    private Boolean force;

    public DeleteDataFrameTransformRequest(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public Boolean getForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (id == null) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationError("data frame transform id must not be null");
            return Optional.of(validationException);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, force);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DeleteDataFrameTransformRequest other = (DeleteDataFrameTransformRequest) obj;
        return Objects.equals(id, other.id) && Objects.equals(force, other.force);
    }
}
