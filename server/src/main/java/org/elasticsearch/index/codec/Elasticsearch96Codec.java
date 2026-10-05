/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.lucene104.Lucene104Codec;

/**
 * {@link ElasticsearchCodec} over {@link Lucene104Codec}, backing {@link PerFieldMapperCodec} and so {@code index.codec=default}.
 */
public class Elasticsearch96Codec extends ElasticsearchCodec {

    public static final String NAME = "Elasticsearch96";

    /** Public no-arg constructor, needed for SPI loading at read-time. */
    public Elasticsearch96Codec() {
        this(Lucene104Codec.Mode.BEST_SPEED);
    }

    public Elasticsearch96Codec(Lucene104Codec.Mode mode) {
        this(mode, ElasticsearchStoredFieldsFormat.Mode.LUCENE);
    }

    public Elasticsearch96Codec(Lucene104Codec.Mode luceneMode, ElasticsearchStoredFieldsFormat.Mode storedFieldsMode) {
        this(luceneMode, storedFieldsMode, ElasticsearchStoredFieldsFormat.Mode.LUCENE);
    }

    public Elasticsearch96Codec(
        Lucene104Codec.Mode luceneMode,
        ElasticsearchStoredFieldsFormat.Mode storedFieldsMode,
        ElasticsearchStoredFieldsFormat.Mode legacyMode
    ) {
        this(luceneMode, storedFieldsMode, legacyMode, false);
    }

    public Elasticsearch96Codec(
        Lucene104Codec.Mode luceneMode,
        ElasticsearchStoredFieldsFormat.Mode storedFieldsMode,
        ElasticsearchStoredFieldsFormat.Mode legacyMode,
        boolean syntheticId
    ) {
        super(NAME, new Lucene104Codec(luceneMode), storedFieldsMode, legacyMode, syntheticId);
    }
}
