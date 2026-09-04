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
        ElasticsearchStoredFieldsFormat.Mode modeBeforeTheAttribute
    ) {
        this(luceneMode, storedFieldsMode, modeBeforeTheAttribute, false);
    }

    public Elasticsearch96Codec(
        Lucene104Codec.Mode luceneMode,
        ElasticsearchStoredFieldsFormat.Mode storedFieldsMode,
        ElasticsearchStoredFieldsFormat.Mode modeBeforeTheAttribute,
        boolean syntheticId
    ) {
        super("Elasticsearch96", new Lucene104Codec(luceneMode), storedFieldsMode, modeBeforeTheAttribute, syntheticId);
    }
}
