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
 * A {@link MetadataFieldMapper} that provides a specific {@link DocValuesFormat} for its own field rather than using the index's default.
 * The field type's name is expected to start with {@code _}.
 */
public abstract class MetadataDocValuesFieldMapper extends MetadataFieldMapper {

    public MetadataDocValuesFieldMapper(MappedFieldType mappedFieldType) {
        super(requireMetadataField(mappedFieldType));
    }

    private static MappedFieldType requireMetadataField(MappedFieldType mappedFieldType){
        if (mappedFieldType.typeName().startsWith("_")) {
            return mappedFieldType;
        }
        throw new IllegalArgumentException("expected `_` prefix for field [" + mappedFieldType.name() + "]");
    }

    /**
     * @param defaultFormat the format that would otherwise be used for this field
     * @return the format to use for this field; may return {@code defaultFormat} unchanged but must not return {@code null}
     */
    public abstract DocValuesFormat getDocValuesFormatForField(DocValuesFormat defaultFormat);
}
