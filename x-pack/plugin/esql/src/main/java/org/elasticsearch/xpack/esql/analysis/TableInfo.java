/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.plan.TableIdentifier;

public class TableInfo {

    private final TableIdentifier id;

    public TableInfo(TableIdentifier id) {
        this.id = id;
    }

    public TableIdentifier id() {
        return id;
    }
}
