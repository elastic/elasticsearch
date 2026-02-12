/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.lakehouse;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;

/**
 * Factory for creating FormatReader instances.
 * Used to provide format readers in a lazy manner.
 *
 * <p>Origin: PR #141678 ({@code org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory}).
 * Changes: package rename only.
 */
@FunctionalInterface
public interface FormatReaderFactory {

    /**
     * Creates a new FormatReader instance.
     *
     * @param settings the node settings
     * @param blockFactory the block factory for creating compute engine blocks
     * @return a new FormatReader instance
     */
    FormatReader create(Settings settings, BlockFactory blockFactory);
}
