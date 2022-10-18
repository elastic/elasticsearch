/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90CompoundFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90LiveDocsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90NormsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90PointsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90SegmentInfoFormat;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90TermVectorsFormat;
import org.apache.lucene.codecs.lucene94.Lucene94FieldInfosFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.elasticsearch.index.codec.vectors.XLucene94HnswVectorsFormat;

import java.util.Objects;

/**
 * Implements the Lucene 9.4 index format
 *
 * <p>If you want to reuse functionality of this codec in another codec, extend {@link org.apache.lucene.codecs.FilterCodec}.
 *
 * @see org.apache.lucene.codecs.lucene94 package documentation for file format details.
 *
 * NOTE: this class was temporarily copied from Lucene to fix a bug in Lucene94HnswVectorsReader.
 * It contains no modifications to the Lucene version except that it returns {@link XLucene94HnswVectorsFormat}.
 */
public class XLucene94Codec extends Codec {

    /** Configuration option for the codec. */
    public enum Mode {
        /** Trade compression ratio for retrieval speed. */
        BEST_SPEED(Lucene90StoredFieldsFormat.Mode.BEST_SPEED),
        /** Trade retrieval speed for compression ratio. */
        BEST_COMPRESSION(Lucene90StoredFieldsFormat.Mode.BEST_COMPRESSION);

        final Lucene90StoredFieldsFormat.Mode storedMode;

        Mode(Lucene90StoredFieldsFormat.Mode storedMode) {
            this.storedMode = Objects.requireNonNull(storedMode);
        }
    }

    private final TermVectorsFormat vectorsFormat = new Lucene90TermVectorsFormat();
    private final FieldInfosFormat fieldInfosFormat = new Lucene94FieldInfosFormat();
    private final SegmentInfoFormat segmentInfosFormat = new Lucene90SegmentInfoFormat();
    private final LiveDocsFormat liveDocsFormat = new Lucene90LiveDocsFormat();
    private final CompoundFormat compoundFormat = new Lucene90CompoundFormat();
    private final NormsFormat normsFormat = new Lucene90NormsFormat();

    private final PostingsFormat defaultPostingsFormat;
    private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
            return XLucene94Codec.this.getPostingsFormatForField(field);
        }
    };

    private final DocValuesFormat defaultDVFormat;
    private final DocValuesFormat docValuesFormat = new PerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return XLucene94Codec.this.getDocValuesFormatForField(field);
        }
    };

    private final KnnVectorsFormat defaultKnnVectorsFormat;
    private final KnnVectorsFormat knnVectorsFormat = new PerFieldKnnVectorsFormat() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return XLucene94Codec.this.getKnnVectorsFormatForField(field);
        }
    };

    private final StoredFieldsFormat storedFieldsFormat;

    /** Instantiates a new codec. */
    public XLucene94Codec() {
        this(XLucene94Codec.Mode.BEST_SPEED);
    }

    /**
     * Instantiates a new codec, specifying the stored fields compression mode to use.
     *
     * @param mode stored fields compression mode to use for newly flushed/merged segments.
     */
    public XLucene94Codec(XLucene94Codec.Mode mode) {
        super("Lucene94");
        this.storedFieldsFormat = new Lucene90StoredFieldsFormat(Objects.requireNonNull(mode).storedMode);
        this.defaultPostingsFormat = new Lucene90PostingsFormat();
        this.defaultDVFormat = new Lucene90DocValuesFormat();
        this.defaultKnnVectorsFormat = new XLucene94HnswVectorsFormat();
    }

    @Override
    public final StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public final TermVectorsFormat termVectorsFormat() {
        return vectorsFormat;
    }

    @Override
    public final PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    @Override
    public final SegmentInfoFormat segmentInfoFormat() {
        return segmentInfosFormat;
    }

    @Override
    public final LiveDocsFormat liveDocsFormat() {
        return liveDocsFormat;
    }

    @Override
    public final CompoundFormat compoundFormat() {
        return compoundFormat;
    }

    @Override
    public final PointsFormat pointsFormat() {
        return new Lucene90PointsFormat();
    }

    @Override
    public final KnnVectorsFormat knnVectorsFormat() {
        return knnVectorsFormat;
    }

    /**
     * Returns the postings format that should be used for writing new segments of <code>field</code>.
     *
     * <p>The default implementation always returns "Lucene90".
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
     * <p>The default implementation always returns "Lucene90".
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
     * <p>The default implementation always returns "lucene94".
     *
     * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
     * future version of Lucene are only guaranteed to be able to read the default implementation.
     */
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return defaultKnnVectorsFormat;
    }

    @Override
    public final DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    @Override
    public final NormsFormat normsFormat() {
        return normsFormat;
    }
}
