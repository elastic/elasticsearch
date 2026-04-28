/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Map;

/**
 * Common interface for complete external data source factories.
 * Both API-based connectors (Flight, JDBC) and table-based catalogs (Iceberg)
 * implement this interface, enabling unified resolution and dispatch.
 *
 * Building-block factories (StorageProviderFactory, FormatReaderFactory) are NOT
 * part of this hierarchy — they are composed by the framework for file-based sources.
 */
public interface ExternalSourceFactory {

    String type();

    boolean canHandle(String location);

    SourceMetadata resolveMetadata(String location, Map<String, Object> config);

    default FilterPushdownSupport filterPushdownSupport() {
        return null;
    }

    default SourceOperatorFactoryProvider operatorFactory() {
        return null;
    }

    /**
     * Optional capability for formats whose readers implement {@link AggregateScanReader}
     * — i.e. can compute ungrouped {@code COUNT(*)} / {@code COUNT(field)} / {@code MIN} /
     * {@code MAX} inside the scan, choosing per row group whether to derive results from
     * column statistics or by reading row data. Returns a provider that the local execution
     * planner uses to construct the runtime aggregate-scan operator when
     * {@code PushAggregatesToExternalSource} elects the runtime path.
     * <p>
     * The default implementation returns {@code null}, meaning the format does not support
     * runtime aggregate pushdown; the optimizer rule will preserve the original scan plan.
     */
    default AggregateScanOperatorFactoryProvider aggregateScanOperatorFactory() {
        return null;
    }

    default SplitProvider splitProvider() {
        return SplitProvider.SINGLE;
    }
}
