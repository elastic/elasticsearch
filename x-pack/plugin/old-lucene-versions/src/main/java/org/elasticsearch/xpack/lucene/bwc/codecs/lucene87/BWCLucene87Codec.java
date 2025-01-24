/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc.codecs.lucene87;

import org.apache.lucene.backward_codecs.lucene50.Lucene50CompoundFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50LiveDocsFormat;
import org.apache.lucene.backward_codecs.lucene60.Lucene60FieldInfosFormat;
import org.apache.lucene.backward_codecs.lucene80.Lucene80DocValuesFormat;
import org.apache.lucene.backward_codecs.lucene84.Lucene84PostingsFormat;
import org.apache.lucene.backward_codecs.lucene86.Lucene86SegmentInfoFormat;
import org.apache.lucene.backward_codecs.lucene87.Lucene87StoredFieldsFormat;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.elasticsearch.xpack.lucene.bwc.codecs.BWCCodec;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene86.Lucene86MetadataOnlyPointsFormat;

import java.util.Objects;

/**
 * Implements the Lucene 8.7 index format. Loaded via SPI for indices created/written with Lucene 8.7.0-8.11.3
 * (Elasticsearch [7.10.0-7-17.26]), mounted as archive indices in Elasticsearch 8.x / 9.x.
 */
public class BWCLucene87Codec extends BWCCodec {

    private enum Mode {
        /** Trade compression ratio for retrieval speed. */
        BEST_SPEED(Lucene87StoredFieldsFormat.Mode.BEST_SPEED, Lucene80DocValuesFormat.Mode.BEST_SPEED),
        /** Trade retrieval speed for compression ratio. */
        BEST_COMPRESSION(Lucene87StoredFieldsFormat.Mode.BEST_COMPRESSION, Lucene80DocValuesFormat.Mode.BEST_COMPRESSION);

        /** compression mode for stored fields */
        private final Lucene87StoredFieldsFormat.Mode storedMode;

        /** compression mode for doc value fields */
        private final Lucene80DocValuesFormat.Mode dvMode;

        Mode(Lucene87StoredFieldsFormat.Mode storedMode, Lucene80DocValuesFormat.Mode dvMode) {
            this.storedMode = Objects.requireNonNull(storedMode);
            this.dvMode = Objects.requireNonNull(dvMode);
        }
    }

    @Override
    protected FieldInfosFormat setFieldInfosFormat() {
        return new Lucene60FieldInfosFormat();
    }

    @Override
    protected SegmentInfoFormat setSegmentInfoFormat() {
        return new Lucene86SegmentInfoFormat();
    }

    private final LiveDocsFormat liveDocsFormat = new Lucene50LiveDocsFormat();
    private final CompoundFormat compoundFormat = new Lucene50CompoundFormat();
    private final PointsFormat pointsFormat = new Lucene86MetadataOnlyPointsFormat();
    private final PostingsFormat defaultFormat;

    private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
            return BWCLucene87Codec.this.getPostingsFormatForField(field);
        }
    };

    private final DocValuesFormat docValuesFormat = new PerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return BWCLucene87Codec.this.getDocValuesFormatForField(field);
        }
    };

    private final StoredFieldsFormat storedFieldsFormat;

    // Needed for SPI loading
    @SuppressWarnings("unused")
    public BWCLucene87Codec() {
        this("BWCLucene87Codec", Mode.BEST_COMPRESSION);
    }

    public BWCLucene87Codec(String name, Mode mode) {
        super(name);
        this.storedFieldsFormat = new Lucene87StoredFieldsFormat(mode.storedMode);
        this.defaultFormat = new Lucene84PostingsFormat();
        this.defaultDVFormat = new Lucene80DocValuesFormat(mode.dvMode);
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return segmentInfosFormat;
    }

    @Override
    public final LiveDocsFormat liveDocsFormat() {
        return liveDocsFormat;
    }

    @Override
    public CompoundFormat compoundFormat() {
        return compoundFormat;
    }

    @Override
    public PointsFormat pointsFormat() {
        return pointsFormat;
    }

    /**
     * Returns the postings format that should be used for writing new segments of <code>field</code>.
     *
     * <p>The default implementation always returns "Lucene84".
     *
     * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
     * future version of Lucene are only guaranteed to be able to read the default implementation.
     */
    public PostingsFormat getPostingsFormatForField(String field) {
        return defaultFormat;
    }

    /**
     * Returns the docvalues format that should be used for writing new segments of <code>field</code>
     * .
     *
     * <p>The default implementation always returns "Lucene80".
     *
     * <p><b>WARNING:</b> if you subclass, you are responsible for index backwards compatibility:
     * future version of Lucene are only guaranteed to be able to read the default implementation.
     */
    public DocValuesFormat getDocValuesFormatForField(String field) {
        return defaultDVFormat;
    }

    @Override
    public final DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    private final DocValuesFormat defaultDVFormat;
}
