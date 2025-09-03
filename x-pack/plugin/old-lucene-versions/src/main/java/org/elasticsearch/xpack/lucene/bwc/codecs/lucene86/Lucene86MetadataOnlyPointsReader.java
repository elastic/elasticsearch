/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene86;

import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.ChecksumIndexInput;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene60.MetadataOnlyBKDReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *  This is a fork of {@link org.apache.lucene.backward_codecs.lucene86.Lucene86PointsReader}
 *  Reads the metadata of point values previously written with Lucene86PointsWriter
 */
public final class Lucene86MetadataOnlyPointsReader extends PointsReader {
    final SegmentReadState readState;
    final Map<Integer, PointValues> readers = new HashMap<>();

    public Lucene86MetadataOnlyPointsReader(SegmentReadState readState) throws IOException {
        this.readState = readState;

        String metaFileName = IndexFileNames.segmentFileName(
            readState.segmentInfo.name,
            readState.segmentSuffix,
            Lucene86MetadataOnlyPointsFormat.META_EXTENSION
        );

        boolean success = false;
        try {
            try (
                ChecksumIndexInput metaIn = EndiannessReverserUtil.openChecksumInput(readState.directory, metaFileName, readState.context)
            ) {
                Throwable priorE = null;
                try {
                    CodecUtil.checkIndexHeader(
                        metaIn,
                        Lucene86MetadataOnlyPointsFormat.META_CODEC_NAME,
                        Lucene86MetadataOnlyPointsFormat.VERSION_START,
                        Lucene86MetadataOnlyPointsFormat.VERSION_CURRENT,
                        readState.segmentInfo.getId(),
                        readState.segmentSuffix
                    );

                    while (true) {
                        int fieldNumber = metaIn.readInt();
                        if (fieldNumber == -1) {
                            break;
                        } else if (fieldNumber < 0) {
                            throw new CorruptIndexException("Illegal field number: " + fieldNumber, metaIn);
                        }
                        PointValues reader = new MetadataOnlyBKDReader(metaIn, true);
                        readers.put(fieldNumber, reader);
                    }
                    metaIn.readLong();
                    metaIn.readLong();
                } catch (Throwable t) {
                    priorE = t;
                } finally {
                    CodecUtil.checkFooter(metaIn, priorE);
                }
            }

            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public PointValues getValues(String fieldName) {
        FieldInfo fieldInfo = readState.fieldInfos.fieldInfo(fieldName);
        if (fieldInfo == null) {
            throw new IllegalArgumentException("field=\"" + fieldName + "\" is unrecognized");
        }
        if (fieldInfo.getPointDimensionCount() == 0) {
            throw new IllegalArgumentException("field=\"" + fieldName + "\" did not index point values");
        }

        return readers.get(fieldInfo.number);
    }

    // We only open the metadata field, and do nothing with the other two files (index/data),
    // for which Lucene checks integrity but we don't need to.
    @Override
    public void checkIntegrity() {}

    @Override
    public void close() throws IOException {
        // Free up heap:
        readers.clear();
    }
}
