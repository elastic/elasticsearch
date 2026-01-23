/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.migration;

/**
 * Empty migration context for generic migrations that don't require
 * migration-specific data. Used for single-endpoint migrations that
 * only need the endpoint data itself.
 */
public class EmptyMigrationContext implements MigrationContext {
    @Override
    public ContextType getContextType() {
        return ContextType.EMPTY;
    }

    private EmptyMigrationContext() {}
}
