/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.enrich;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.Strings;

public final class DeletePolicyRequest implements Validatable {

    private final String name;

    public DeletePolicyRequest(String name) {
        if (Strings.hasLength(name) == false) {
            throw new IllegalArgumentException("name must be a non-null and non-empty string");
        }
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
