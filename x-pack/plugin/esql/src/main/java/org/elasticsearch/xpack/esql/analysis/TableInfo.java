/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.plan.IndexPattern;

public class TableInfo {

    private final IndexPattern id;

    public TableInfo(IndexPattern id) {
        this.id = id;
    }

    public IndexPattern id() {
        return id;
    }
}
