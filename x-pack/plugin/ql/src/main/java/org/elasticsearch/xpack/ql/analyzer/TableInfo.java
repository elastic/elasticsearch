/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.analyzer;

import org.elasticsearch.xpack.ql.plan.TableIdentifier;

public class TableInfo {

    private final TableIdentifier id;
    private final boolean isFrozen;

    public TableInfo(TableIdentifier id, boolean isFrozen) {
        this.id = id;
        this.isFrozen = isFrozen;
    }

    public TableIdentifier id() {
        return id;
    }

    public boolean isFrozen() {
        return isFrozen;
    }
}
