/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

/**
 * How much of a dataset its schema is defined over. This is the property that decides whether a schema can be
 * answered from part of the dataset, and it belongs to the dataset's configuration rather than to any query.
 */
public enum SchemaBreadth {

    /** A declared mapping: the schema is the declaration, so no file defines it. */
    DECLARATION,

    /** {@code first_file_wins}: one file defines it, and which file is decided by listing order. */
    ONE_FILE,

    /**
     * {@code union_by_name} and {@code strict}: every file contributes, by contract. Answering from part of the
     * dataset would report a narrower schema than it has, which is the outcome these modes exist to prevent.
     */
    EVERY_FILE;

    public static SchemaBreadth of(FormatReader.SchemaResolution schemaResolution) {
        return switch (schemaResolution) {
            case FIRST_FILE_WINS -> ONE_FILE;
            case UNION_BY_NAME, STRICT -> EVERY_FILE;
        };
    }

    /** Whether a schema this wide can be answered from a prefix of the dataset at all. */
    public boolean answerableFromAPrefix() {
        return this != EVERY_FILE;
    }
}
