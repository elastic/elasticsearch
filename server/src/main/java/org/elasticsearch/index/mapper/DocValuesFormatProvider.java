/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.codecs.DocValuesFormat;

/**
 * Implemented by a {@link Mapper} (including a {@link MetadataFieldMapper}) that needs to select a
 * specific {@link DocValuesFormat} for its own field rather than the index's default. Consulted by
 * {@code org.elasticsearch.index.codec.PerFieldFormatSupplier#getDocValuesFormatForField}, mirroring
 * how {@code DenseVectorFieldMapper#getKnnVectorsFormatForField} customizes its own field's
 * {@code KnnVectorsFormat} — but expressed as an interface rather than a concrete-class check, so
 * mapper implementations that live outside {@code server} (e.g. in a module) can hook in too.
 */
public interface DocValuesFormatProvider {

    /**
     * @param defaultFormat the format that would otherwise be used for this field
     * @return the format to use for this field; may return {@code defaultFormat} unchanged
     */
    DocValuesFormat getDocValuesFormatForField(DocValuesFormat defaultFormat);
}
