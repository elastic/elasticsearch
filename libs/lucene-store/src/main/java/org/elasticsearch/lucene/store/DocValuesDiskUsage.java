/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.store;

import org.apache.lucene.index.FieldInfo;

import java.io.IOException;

/**
 * Implemented by doc values producers that can report the on-disk size of a field from their metadata,
 * without reading any doc values.
 */
public interface DocValuesDiskUsage {

    /** Receives the on-disk size of each part of a field's doc values. */
    interface Visitor {
        /**
         * Bytes of doc values data in the data file.
         *
         * @param component the part of the field that the bytes belong to, for example {@code "values"} or {@code "termsDict"}
         */
        void docValuesData(String component, long bytes);

        /** Bytes of the field's entry in the metadata file. */
        void docValuesMetadata(long bytes);

        /** Bytes of skipper data. */
        void docValuesSkipperData(long bytes);
    }

    /**
     * Reports the on-disk size of the doc values of {@code field} to {@code visitor}.
     *
     * @return {@code false}, without calling the visitor, if this producer cannot report the size of {@code field}
     */
    boolean visitDiskUsage(FieldInfo field, Visitor visitor) throws IOException;
}
