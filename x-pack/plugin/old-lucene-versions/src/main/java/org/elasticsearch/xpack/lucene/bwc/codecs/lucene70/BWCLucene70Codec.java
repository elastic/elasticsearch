/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */

package org.elasticsearch.xpack.lucene.bwc.codecs.lucene70;

import org.apache.lucene.backward_codecs.lucene50.Lucene50CompoundFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50LiveDocsFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.backward_codecs.lucene60.Lucene60FieldInfosFormat;
import org.apache.lucene.backward_codecs.lucene70.Lucene70SegmentInfoFormat;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.elasticsearch.xpack.lucene.bwc.codecs.BWCCodec;

public class BWCLucene70Codec extends BWCCodec {

    private final FieldInfosFormat fieldInfosFormat = wrap(new Lucene60FieldInfosFormat());
    private final SegmentInfoFormat segmentInfosFormat = wrap(new Lucene70SegmentInfoFormat());
    private final LiveDocsFormat liveDocsFormat = new Lucene50LiveDocsFormat();
    private final CompoundFormat compoundFormat = new Lucene50CompoundFormat();
    private final StoredFieldsFormat storedFieldsFormat;

    public BWCLucene70Codec() {
        super("BWCLucene70Codec");
        storedFieldsFormat = new Lucene50StoredFieldsFormat(Lucene50StoredFieldsFormat.Mode.BEST_SPEED);
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return segmentInfosFormat;
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
}
