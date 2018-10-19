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
package org.elasticsearch.client.watcher;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.lucene.uid.Versions;

import java.util.Optional;

/**
 * A delete watch request to delete an watch by name (id)
 */
public class DeleteWatchRequest implements Validatable {

    private String id;
    private long version = Versions.MATCH_ANY;

    public DeleteWatchRequest() {
        this(null);
    }

    public DeleteWatchRequest(String id) {
        this.id = id;
    }

    /**
     * @return The name of the watch to be deleted
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the name of the watch to be deleted
     */
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public Optional<ValidationException> validate() {
        ValidationException exception = new ValidationException();
        if (id == null) {
            exception.addValidationError("watch id is missing");
        } else if (PutWatchRequest.isValidId(id) == false) {
            exception.addValidationError("watch id contains whitespace");
        }
        return exception.validationErrors().isEmpty() ? Optional.empty() : Optional.of(exception);
    }

    @Override
    public String toString() {
        return "delete [" + id + "]";
    }
}
