/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.codecs.lucene103.Lucene103PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.elasticsearch.index.codec.perfield.XPerFieldDocValuesFormat;
import org.elasticsearch.index.codec.storedfields.ESStoredFieldsFormat;
import org.elasticsearch.index.codec.storedfields.ESZstd814StoredFieldsFormat;
import org.elasticsearch.index.codec.storedfields.PerFieldStoredFieldsFormat;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;

public class Elasticsearch93ZstdCompressionLucene103Codec extends CodecService.DeduplicateFieldInfosCodec {

    static final PostingsFormat DEFAULT_POSTINGS_FORMAT = new Lucene103PostingsFormat();

    private final PostingsFormat defaultPostingsFormat;
    private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
            return Elasticsearch93ZstdCompressionLucene103Codec.this.getPostingsFormatForField(field);
        }
    };

    private final DocValuesFormat defaultDVFormat;
    private final DocValuesFormat docValuesFormat = new XPerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return Elasticsearch93ZstdCompressionLucene103Codec.this.getDocValuesFormatForField(field);
        }
    };

    private final KnnVectorsFormat defaultKnnVectorsFormat;
    private final KnnVectorsFormat knnVectorsFormat = new PerFieldKnnVectorsFormat() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return Elasticsearch93ZstdCompressionLucene103Codec.this.getKnnVectorsFormatForField(field);
        }
    };

    // TODO: fix this
    protected final ESStoredFieldsFormat defaultStoredFieldsFormat;
    private final StoredFieldsFormat storedFieldsFormat = new PerFieldStoredFieldsFormat() {
        @Override
        protected ESStoredFieldsFormat getStoredFieldsFormatForField(String field) {
            return Elasticsearch93ZstdCompressionLucene103Codec.this.getStoredFieldsFormatForField(field);
        }
    };

    /** Public no-arg constructor, needed for SPI loading at read-time. */
    public Elasticsearch93ZstdCompressionLucene103Codec() {
        this(Zstd814StoredFieldsFormat.Mode.BEST_SPEED);
    }

    /**
     * Constructor. Takes a {@link Zstd814StoredFieldsFormat.Mode} that describes whether to optimize for retrieval speed at the expense of
     * worse space-efficiency or vice-versa.
     */
    public Elasticsearch93ZstdCompressionLucene103Codec(Zstd814StoredFieldsFormat.Mode mode) {
        super("Elasticsearch93ZstdStoredFieldsLucene103Codec", new Lucene103Codec());
        this.defaultStoredFieldsFormat = new ESZstd814StoredFieldsFormat(mode);
        this.defaultPostingsFormat = DEFAULT_POSTINGS_FORMAT;
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
     * <p>The default implementation always returns "Lucene912".
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
     * <p>The default implementation always returns "Lucene912".
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
     * <p>The default implementation always returns "Lucene912".
     *
     * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
     * future version of Lucene are only guaranteed to be able to read the default implementation.
     */
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return defaultKnnVectorsFormat;
    }

    // We need to return ES... for the SPI loading
    public ESStoredFieldsFormat getStoredFieldsFormatForField(String field) {
        return defaultStoredFieldsFormat;
    }
}
