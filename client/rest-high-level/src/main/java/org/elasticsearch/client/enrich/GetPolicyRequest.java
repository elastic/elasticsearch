/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.enrich;

import org.elasticsearch.client.Validatable;

import java.util.List;

public final class GetPolicyRequest implements Validatable {

    private final List<String> names;

    public GetPolicyRequest() {
        this(List.of());
    }

    public GetPolicyRequest(String... names) {
        this(List.of(names));
    }

    public GetPolicyRequest(List<String> names) {
        this.names = names;
    }

    public List<String> getNames() {
        return names;
    }
}
