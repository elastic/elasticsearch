/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.client.Validatable;

public class DeleteDataStreamRequest implements Validatable {

    private final String name;

    public DeleteDataStreamRequest(String name) {
        if (name == null) {
            throw new IllegalArgumentException("The data stream name cannot be null.");
        }
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
