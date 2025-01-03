/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc.codecs.lucene80;

import org.apache.lucene.backward_codecs.lucene50.Lucene50CompoundFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50LiveDocsFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50TermVectorsFormat;
import org.apache.lucene.backward_codecs.lucene60.Lucene60FieldInfosFormat;
import org.apache.lucene.backward_codecs.lucene60.Lucene60PointsFormat;
import org.apache.lucene.backward_codecs.lucene70.Lucene70SegmentInfoFormat;
import org.apache.lucene.backward_codecs.lucene80.Lucene80NormsFormat;
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
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.elasticsearch.xpack.lucene.bwc.codecs.BWCCodec;

public class BWCLucene80Codec extends BWCCodec {

    private final TermVectorsFormat vectorsFormat = new Lucene50TermVectorsFormat();
    private final FieldInfosFormat fieldInfosFormat = wrap(new Lucene60FieldInfosFormat());
    private final SegmentInfoFormat segmentInfosFormat = wrap(new Lucene70SegmentInfoFormat());
    private final LiveDocsFormat liveDocsFormat = new Lucene50LiveDocsFormat();
    private final CompoundFormat compoundFormat = new Lucene50CompoundFormat();

    private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
            throw new UnsupportedOperationException("Old codecs can't be used for writing");
        }
    };

    private final DocValuesFormat docValuesFormat = new PerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return defaultDVFormat;
        }
    };
    private final DocValuesFormat defaultDVFormat = DocValuesFormat.forName("Lucene80");

    private final StoredFieldsFormat storedFieldsFormat;

    /** Instantiates a new codec. */
    public BWCLucene80Codec() {
        super("BWCLucene80Codec");
        this.storedFieldsFormat = new Lucene50StoredFieldsFormat();
    }

    @Override
    public final StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
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
        return new Lucene60PointsFormat();
    }

    @Override
    public final DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    private final NormsFormat normsFormat = new Lucene80NormsFormat();

    @Override
    public final NormsFormat normsFormat() {
        return normsFormat;
    }

    @Override
    public final KnnVectorsFormat knnVectorsFormat() {
        return KnnVectorsFormat.EMPTY;
    }
}
