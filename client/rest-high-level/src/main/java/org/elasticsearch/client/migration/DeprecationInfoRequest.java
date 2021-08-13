/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.migration;

import org.elasticsearch.client.Validatable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DeprecationInfoRequest implements Validatable {

    private final List<String> indices;

    public DeprecationInfoRequest(List<String> indices) {
        this.indices = Collections.unmodifiableList(Objects.requireNonNull(indices, "indices cannot be null"));
    }

    public DeprecationInfoRequest() {
        this.indices = Collections.unmodifiableList(Collections.emptyList());
    }

    public List<String> getIndices() {
        return indices;
    }
}
