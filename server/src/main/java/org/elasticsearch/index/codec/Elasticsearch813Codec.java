/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.elasticsearch.index.codec.zstd.Zstd813StoredFieldsFormat;

/**
 * Elasticsearch codec as of 8.13. This extends the Lucene 9.9 codec to compressed stored fields with ZSTD instead of LZ4/DEFLATE. See
 * {@link Zstd813StoredFieldsFormat}.
 */
public class Elasticsearch813Codec extends FilterCodec {

    private final StoredFieldsFormat storedFieldsFormat;

    private final PostingsFormat defaultPostingsFormat;
    private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
            return Elasticsearch813Codec.this.getPostingsFormatForField(field);
        }
    };

    private final DocValuesFormat defaultDVFormat;
    private final DocValuesFormat docValuesFormat = new PerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return Elasticsearch813Codec.this.getDocValuesFormatForField(field);
        }
    };

    private final KnnVectorsFormat defaultKnnVectorsFormat;
    private final KnnVectorsFormat knnVectorsFormat = new PerFieldKnnVectorsFormat() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return Elasticsearch813Codec.this.getKnnVectorsFormatForField(field);
        }
    };

    /** Public no-arg constructor, needed for SPI loading at read-time. */
    public Elasticsearch813Codec() {
        this(Zstd813StoredFieldsFormat.Mode.BEST_SPEED);
    }

    /**
     * Constructor. Takes a {@link Zstd813StoredFieldsFormat.Mode} that describes whether to optimize for retrieval speed at the expense of worse space-efficiency or vice-versa.
     */
    public Elasticsearch813Codec(Zstd813StoredFieldsFormat.Mode mode) {
        super("Elasticsearch813", new Lucene99Codec());
        this.storedFieldsFormat = new Zstd813StoredFieldsFormat(mode);
        this.defaultPostingsFormat = new Lucene99PostingsFormat();
        this.defaultDVFormat = new Lucene90DocValuesFormat();
        this.defaultKnnVectorsFormat = new Lucene99HnswVectorsFormat();
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public final PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    @Override
    public final DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    @Override
    public final KnnVectorsFormat knnVectorsFormat() {
        return knnVectorsFormat;
    }

    /**
     * Returns the postings format that should be used for writing new segments of <code>field</code>.
     *
     * <p>The default implementation always returns "Lucene99".
     *
     * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
     * future version of Lucene are only guaranteed to be able to read the default implementation,
     */
    public PostingsFormat getPostingsFormatForField(String field) {
        return defaultPostingsFormat;
    }

    /**
     * Returns the docvalues format that should be used for writing new segments of <code>field</code>
     * .
     *
     * <p>The default implementation always returns "Lucene99".
     *
     * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
     * future version of Lucene are only guaranteed to be able to read the default implementation.
     */
    public DocValuesFormat getDocValuesFormatForField(String field) {
        return defaultDVFormat;
    }

    /**
     * Returns the vectors format that should be used for writing new segments of <code>field</code>
     *
     * <p>The default implementation always returns "Lucene95".
     *
     * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
     * future version of Lucene are only guaranteed to be able to read the default implementation.
     */
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return defaultKnnVectorsFormat;
    }
}
