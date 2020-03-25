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

import java.util.Objects;

/**
 * A request to explicitly activate a watch.
 */
public final class ActivateWatchRequest implements Validatable {

    private final String watchId;

    public ActivateWatchRequest(String watchId) {
        this.watchId  = Objects.requireNonNull(watchId, "Watch identifier is required");
        if (PutWatchRequest.isValidId(this.watchId) == false) {
            throw new IllegalArgumentException("Watch identifier contains whitespace");
        }
    }

    /**
     * @return The ID of the watch to be activated.
     */
    public String getWatchId() {
        return watchId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ActivateWatchRequest that = (ActivateWatchRequest) o;
        return Objects.equals(watchId, that.watchId);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(watchId);
        return result;
    }
}
