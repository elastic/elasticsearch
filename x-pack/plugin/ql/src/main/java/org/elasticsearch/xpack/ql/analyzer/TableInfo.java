/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.analyzer;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.ql.options.EsSourceOptions;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;

public class TableInfo {

    private final TableIdentifier id;
    private final boolean isFrozen;
    @Nullable
    private final EsSourceOptions esSourceOptions;

    public TableInfo(TableIdentifier id, boolean isFrozen) {
        this(id, isFrozen, null);
    }

    public TableInfo(TableIdentifier id, boolean isFrozen, EsSourceOptions esSourceOptions) {
        this.id = id;
        this.isFrozen = isFrozen;
        this.esSourceOptions = esSourceOptions;
    }

    public TableIdentifier id() {
        return id;
    }

    public boolean isFrozen() {
        return isFrozen;
    }

    public EsSourceOptions esSourceOptions() {
        return esSourceOptions;
    }
}
