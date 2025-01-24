/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc.codecs;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.elasticsearch.test.ESTestCase;

public class BWCCodecTests extends ESTestCase {

    private final Codec codec;

    public BWCCodecTests() {
        this.codec = new BWCCodec("WrapperCodec") {
            @Override
            protected SegmentInfoFormat setSegmentInfoFormat() {
                return null;
            }

            @Override
            protected FieldInfosFormat setFieldInfosFormat() {
                return null;
            }

            @Override
            public PostingsFormat postingsFormat() {
                return null;
            }

            @Override
            public DocValuesFormat docValuesFormat() {
                return null;
            }

            @Override
            public StoredFieldsFormat storedFieldsFormat() {
                return null;
            }

            @Override
            public FieldInfosFormat fieldInfosFormat() {
                return null;
            }

            @Override
            public SegmentInfoFormat segmentInfoFormat() {
                return null;
            }

            @Override
            public LiveDocsFormat liveDocsFormat() {
                return null;
            }

            @Override
            public CompoundFormat compoundFormat() {
                return null;
            }

            @Override
            public PointsFormat pointsFormat() {
                return null;
            }
        };
    }

    public void testNormsFormatUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class, codec::normsFormat);
    }

    public void testTermVectorsFormatUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class, codec::termVectorsFormat);
    }

    public void testKnnVectorsFormatUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class, codec::knnVectorsFormat);
    }
}
