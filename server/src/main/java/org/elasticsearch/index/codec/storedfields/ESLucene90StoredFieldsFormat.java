/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.storedfields;

import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter;

import java.util.Set;

/**
 * Simple wrapper for Lucene90StoredFieldsFormat that allows it to be loaded through SPI
 */
public class ESLucene90StoredFieldsFormat extends FilterESStoredFieldsFormat {
    public static final Set<String> FILE_EXTENSIONS = Set.of(
        Lucene90CompressingStoredFieldsWriter.FIELDS_EXTENSION,
        Lucene90CompressingStoredFieldsWriter.INDEX_EXTENSION,
        Lucene90CompressingStoredFieldsWriter.META_EXTENSION
    );
    public static final String FORMAT_NAME = "ESLucene90StoredFieldsFormat";

    public ESLucene90StoredFieldsFormat() {
        this(Lucene103Codec.Mode.BEST_SPEED);
    }

    public ESLucene90StoredFieldsFormat(Lucene103Codec.Mode mode) {
        super(
            FORMAT_NAME,
            new Lucene90StoredFieldsFormat(
                mode == Lucene103Codec.Mode.BEST_COMPRESSION
                    ? Lucene90StoredFieldsFormat.Mode.BEST_COMPRESSION
                    : Lucene90StoredFieldsFormat.Mode.BEST_SPEED
            )
        );
    }

    @Override
    protected Set<String> getFileExtensions() {
        return FILE_EXTENSIONS;
    }
}
