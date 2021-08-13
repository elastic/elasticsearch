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
 * A request to delete a machine learning filter
 */
public class DeleteFilterRequest implements Validatable {

    private final String filterId;

    public DeleteFilterRequest(String filterId) {
        this.filterId = Objects.requireNonNull(filterId, "[filter_id] is required");
    }

    public String getId() {
        return filterId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(filterId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final DeleteFilterRequest other = (DeleteFilterRequest) obj;

        return Objects.equals(filterId, other.filterId);
    }
}
