/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Map;

/**
 * Marker {@link FormatReader} for readers that recognise no per-query configuration keys.
 * Supplies the standard "no keys claimed" override of {@link #withConfigTrackingConsumedKeys}
 * so the dozen-plus test fixtures and stateless production readers (ORC, JMH benchmark stubs)
 * do not each repeat the same {@code return Configured.empty(this)} body.
 *
 * <p>Implement this in place of {@link FormatReader} when the reader genuinely has no per-query
 * keys today. Readers that <i>do</i> claim keys must still implement {@link FormatReader} directly
 * — the abstract contract there is the forcing function preventing silent acceptance of unknown
 * configuration keys.
 *
 * <p>Symmetric to {@link StorageProviderFactory#noConfigKeys(java.util.function.Supplier)} on the
 * storage side: both expose a single explicit opt-in point for the "no per-query options" case.
 */
public interface NoConfigFormatReader extends FormatReader {

    @Override
    default Configured<FormatReader> withConfigTrackingConsumedKeys(Map<String, Object> config) {
        return Configured.empty(this);
    }
}
