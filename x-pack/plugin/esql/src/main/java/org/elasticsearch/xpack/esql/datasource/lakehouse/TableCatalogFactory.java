/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.lakehouse;

import org.elasticsearch.common.settings.Settings;

/**
 * Factory for creating TableCatalog instances.
 * Used to provide table catalogs in a lazy manner.
 *
 * <p>Origin: PR #141678 ({@code org.elasticsearch.xpack.esql.datasources.spi.TableCatalogFactory}).
 * Changes: package rename only.
 */
@FunctionalInterface
public interface TableCatalogFactory {

    /**
     * Creates a new TableCatalog instance.
     *
     * @param settings the node settings
     * @return a new TableCatalog instance
     */
    TableCatalog create(Settings settings);
}
