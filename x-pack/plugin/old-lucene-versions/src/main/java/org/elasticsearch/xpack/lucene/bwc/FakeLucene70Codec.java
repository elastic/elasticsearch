/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc;

import org.apache.lucene.backward_codecs.lucene50.Lucene50CompoundFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50LiveDocsFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.backward_codecs.lucene60.Lucene60FieldInfosFormat;
import org.apache.lucene.backward_codecs.lucene70.Lucene70SegmentInfoFormat;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90Codec;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FakeLucene70Codec extends FilterCodec {

    public FakeLucene70Codec() {
        super("FakeLucene70Codec", new Lucene90Codec());
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return new Lucene50StoredFieldsFormat();
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        final FieldInfosFormat wrapped = new Lucene60FieldInfosFormat();
        return new FieldInfosFormat() {
            @Override
            public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext iocontext)
                throws IOException {
                return filterFields(wrapped.read(directory, segmentInfo, segmentSuffix, iocontext));
            }

            @Override
            public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos, IOContext context)
                throws IOException {
                wrapped.write(directory, segmentInfo, segmentSuffix, infos, context);
            }
        };
    }

    // mark all fields as having no postings, no doc values, and no points.
    private FieldInfos filterFields(FieldInfos fieldInfos) {
        List<FieldInfo> fieldInfoCopy = new ArrayList<>(fieldInfos.size());
        for (FieldInfo fieldInfo : fieldInfos) {
            fieldInfoCopy.add(
                new FieldInfo(
                    fieldInfo.name,
                    fieldInfo.number,
                    false,
                    false,
                    false,
                    IndexOptions.NONE,
                    DocValuesType.NONE,
                    -1,
                    fieldInfo.attributes(),
                    0,
                    0,
                    0,
                    0,
                    fieldInfo.getVectorSimilarityFunction(),
                    fieldInfo.isSoftDeletesField()
                )
            );
        }
        FieldInfos newFieldInfos = new FieldInfos(fieldInfoCopy.toArray(new FieldInfo[0]));
        return newFieldInfos;
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        final SegmentInfoFormat wrapped = new Lucene70SegmentInfoFormat();
        return new SegmentInfoFormat() {
            @Override
            public SegmentInfo read(Directory directory, String segmentName, byte[] segmentID, IOContext context) throws IOException {
                return adaptVersion(wrapped.read(directory, segmentName, segmentID, context));
            }

            @Override
            public void write(Directory dir, SegmentInfo info, IOContext ioContext) throws IOException {
                wrapped.write(dir, info, ioContext);
            }
        };
    }

    private SegmentInfo adaptVersion(SegmentInfo segmentInfo) {
        return OldLuceneVersions.wrap(segmentInfo);
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
        return new Lucene50LiveDocsFormat();
    }

    @Override
    public CompoundFormat compoundFormat() {
        return new Lucene50CompoundFormat();
    }
}
