/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
