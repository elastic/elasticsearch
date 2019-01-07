/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import java.util.List;
import java.util.function.Supplier;

/**
 * Abstraction used to make sure that we lazily load authorized indices only when requested and only maximum once per request. Also
 * makes sure that authorized indices don't get updated throughout the same request for the same user.
 */
class AuthorizedIndices {

    private final Supplier<List<String>> supplier;
    private List<String> authorizedIndices;

    AuthorizedIndices(Supplier<List<String>> authorizedIndicesSupplier) {
        this.supplier = authorizedIndicesSupplier;
    }

    List<String> get() {
        if (authorizedIndices == null) {
            authorizedIndices = supplier.get();
        }
        return authorizedIndices;
    }
}
