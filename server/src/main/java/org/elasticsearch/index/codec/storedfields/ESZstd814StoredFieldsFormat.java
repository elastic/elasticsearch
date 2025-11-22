/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.storedfields;

import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;

import java.util.Set;

/**
 * Simple wrapper for Lucene90StoredFieldsFormat that uses zstd for compression. Allowing to be loaded through SPI.
 */
public class ESZstd814StoredFieldsFormat extends FilterESStoredFieldsFormat {
    public static final Set<String> FILE_EXTENSIONS = Set.of(
        Lucene90CompressingStoredFieldsWriter.FIELDS_EXTENSION,
        Lucene90CompressingStoredFieldsWriter.INDEX_EXTENSION,
        Lucene90CompressingStoredFieldsWriter.META_EXTENSION
    );

    public ESZstd814StoredFieldsFormat() {
        this(Zstd814StoredFieldsFormat.Mode.BEST_SPEED);
    }

    public ESZstd814StoredFieldsFormat(Zstd814StoredFieldsFormat.Mode mode) {
        super("ESZstd814StoredFieldsFormat", mode.getFormat());
    }

    @Override
    protected Set<String> getFileExtensions() {
        return FILE_EXTENSIONS;
    }
}
