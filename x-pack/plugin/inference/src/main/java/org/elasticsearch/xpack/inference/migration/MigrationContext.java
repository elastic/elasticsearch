/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.migration;

/**
 * Context interface for passing migration-specific data to batch migrations.
 * Different migration types can use different context implementations to pass
 * the data they need (e.g., authorization models, service-specific data).
 */
public interface MigrationContext {
    /**
     * Returns the type of this context, which can be used to safely cast
     * the context to the appropriate implementation type.
     *
     * @return the context type
     */
    ContextType getContextType();

    /**
     * Enumeration of supported context types for type-safe context access.
     */
    enum ContextType {
        EIS,
        EMPTY
    }
}
