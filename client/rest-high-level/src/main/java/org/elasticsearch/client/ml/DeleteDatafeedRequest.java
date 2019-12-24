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
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;

import java.util.Objects;

/**
 * Request to delete a Machine Learning Datafeed via its ID
 */
public class DeleteDatafeedRequest implements Validatable {

    private String datafeedId;
    private Boolean force;

    public DeleteDatafeedRequest(String datafeedId) {
        this.datafeedId = Objects.requireNonNull(datafeedId, "[datafeed_id] must not be null");
    }

    public String getDatafeedId() {
        return datafeedId;
    }

    public Boolean getForce() {
        return force;
    }

    /**
     * Used to forcefully delete a started datafeed.
     * This method is quicker than stopping and deleting the datafeed.
     *
     * @param force When {@code true} forcefully delete a started datafeed. Defaults to {@code false}
     */
    public void setForce(Boolean force) {
        this.force = force;
    }

    @Override
    public int hashCode() {
        return Objects.hash(datafeedId, force);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }

        DeleteDatafeedRequest other = (DeleteDatafeedRequest) obj;
        return Objects.equals(datafeedId, other.datafeedId) && Objects.equals(force, other.force);
    }

}
