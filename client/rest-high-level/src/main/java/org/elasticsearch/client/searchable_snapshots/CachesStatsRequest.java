/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.searchable_snapshots;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;

import java.util.Optional;

public class CachesStatsRequest implements Validatable {

    private final String[] nodesIds;

    public CachesStatsRequest(String... nodesIds) {
        this.nodesIds = nodesIds;
    }

    public String[] getNodesIds() {
        return nodesIds;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (nodesIds != null) {
            for (String nodeId : nodesIds) {
                if (nodeId == null || nodeId.isEmpty()) {
                    final ValidationException validation = new ValidationException();
                    validation.addValidationError("Node ids cannot be null or empty");
                    return Optional.of(validation);
                }
            }
        }
        return Optional.empty();
    }
}
