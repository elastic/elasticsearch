/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;

/**
 * Factory for creating {@link FormatReader} instances.
 * This functional interface allows data source plugins to provide
 * format reader implementations without exposing implementation details.
 */
@FunctionalInterface
public interface FormatReaderFactory {

    /**
     * Creates a new format reader instance.
     *
     * @param settings Elasticsearch settings for configuration
     * @param blockFactory factory for creating data blocks
     * @return a new format reader instance
     */
    FormatReader create(Settings settings, BlockFactory blockFactory);
}
