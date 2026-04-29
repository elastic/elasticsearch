/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.List;
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

    /**
     * Resolves the full file layout (metadata + split ranges) for a single file in one pass.
     * The default implementation defers to {@link #resolveMetadata} and returns no split ranges,
     * preserving backward compatibility for non-file factories (connectors, table catalogs).
     * <p>
     * File-based factories that work with range-aware (columnar) formats should override this
     * to read the file's footer once and emit both schema/statistics and split ranges, avoiding
     * the double-read that would otherwise happen across {@code resolveMetadata} and
     * {@link RangeAwareFormatReader#resolveFileLayout}.
     */
    default FileLayout resolveFileLayout(String location, Map<String, Object> config) {
        return new FileLayout(resolveMetadata(location, config), List.of());
    }

    default FilterPushdownSupport filterPushdownSupport() {
        return null;
    }

    default SourceOperatorFactoryProvider operatorFactory() {
        return null;
    }

    default SplitProvider splitProvider() {
        return SplitProvider.SINGLE;
    }
}
