/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc.codecs.lucene70;

import org.apache.lucene.backward_codecs.lucene50.Lucene50CompoundFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50LiveDocsFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.backward_codecs.lucene60.Lucene60FieldInfosFormat;
import org.apache.lucene.backward_codecs.lucene70.Lucene70SegmentInfoFormat;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.elasticsearch.xpack.lucene.bwc.codecs.BWCCodec;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene60.Lucene60MetadataOnlyPointsFormat;

/**
 * Implements the Lucene 7.0 index format. Loaded via SPI for indices created/written with Lucene 7.x (Elasticsearch 6.x) mounted
 * as archive indices first in Elasticsearch 8.x. Lucene 9.12 retained Lucene70Codec in its classpath which required overriding the
 * codec name and version in the segment infos. This codec is still needed after upgrading to Elasticsearch 9.x because its codec
 * name has been written to disk.
 */
public class BWCLucene70Codec extends BWCCodec {

    private final LiveDocsFormat liveDocsFormat = new Lucene50LiveDocsFormat();
    private final CompoundFormat compoundFormat = new Lucene50CompoundFormat();
    private final StoredFieldsFormat storedFieldsFormat;
    private final DocValuesFormat defaultDVFormat = new Lucene70DocValuesFormat();
    private final DocValuesFormat docValuesFormat = new PerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return defaultDVFormat;
        }
    };
    private final PointsFormat pointsFormat = new Lucene60MetadataOnlyPointsFormat();

    // Needed for SPI loading
    @SuppressWarnings("unused")
    public BWCLucene70Codec() {
        this("BWCLucene70Codec");
    }

    protected BWCLucene70Codec(String name) {
        super(name);
        storedFieldsFormat = new Lucene50StoredFieldsFormat(Lucene50StoredFieldsFormat.Mode.BEST_SPEED);
    }

    @Override
    protected FieldInfosFormat originalFieldInfosFormat() {
        return new Lucene60FieldInfosFormat();
    }

    @Override
    protected SegmentInfoFormat originalSegmentInfoFormat() {
        return new Lucene70SegmentInfoFormat();
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
        return liveDocsFormat;
    }

    @Override
    public CompoundFormat compoundFormat() {
        return compoundFormat;
    }

    @Override
    public final DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    @Override
    public PointsFormat pointsFormat() {
        return pointsFormat;
    }
}
