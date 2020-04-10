/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.xpack.ql.plan.TableIdentifier;

public class TableInfo {

    private final TableIdentifier id;
    private final boolean isFrozen;

    TableInfo(TableIdentifier id, boolean isFrozen) {
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
