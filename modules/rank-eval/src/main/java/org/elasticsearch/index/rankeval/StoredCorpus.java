/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import java.util.List;

public class StoredCorpus {
    private final String id;
    private final String name;
    private final String description;
    private final List<String> indices;

    public StoredCorpus(String id, String name, String description, List<String> indices) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.indices = indices;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public List<String> getIndices() {
        return indices;
    }
}
