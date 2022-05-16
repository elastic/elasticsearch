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
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene60;

import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Reads the metadata of point values previously written with Lucene60PointsWriter */
public class Lucene60MetadataOnlyPointsReader extends PointsReader {
    final IndexInput dataIn;
    final SegmentReadState readState;
    final Map<Integer, PointValues> readers = new HashMap<>();

    /** Sole constructor */
    public Lucene60MetadataOnlyPointsReader(SegmentReadState readState) throws IOException {
        this.readState = readState;

        String indexFileName = IndexFileNames.segmentFileName(
            readState.segmentInfo.name,
            readState.segmentSuffix,
            Lucene60MetadataOnlyPointsFormat.INDEX_EXTENSION
        );

        Map<Integer, Long> fieldToFileOffset = new HashMap<>();

        // Read index file
        try (ChecksumIndexInput indexIn = EndiannessReverserUtil.openChecksumInput(readState.directory, indexFileName, readState.context)) {
            Throwable priorE = null;
            try {
                CodecUtil.checkIndexHeader(
                    indexIn,
                    Lucene60MetadataOnlyPointsFormat.META_CODEC_NAME,
                    Lucene60MetadataOnlyPointsFormat.INDEX_VERSION_START,
                    Lucene60MetadataOnlyPointsFormat.INDEX_VERSION_CURRENT,
                    readState.segmentInfo.getId(),
                    readState.segmentSuffix
                );
                int count = indexIn.readVInt();
                for (int i = 0; i < count; i++) {
                    int fieldNumber = indexIn.readVInt();
                    long fp = indexIn.readVLong();
                    fieldToFileOffset.put(fieldNumber, fp);
                }
            } catch (Throwable t) {
                priorE = t;
            } finally {
                CodecUtil.checkFooter(indexIn, priorE);
            }
        }

        String dataFileName = IndexFileNames.segmentFileName(
            readState.segmentInfo.name,
            readState.segmentSuffix,
            Lucene60MetadataOnlyPointsFormat.DATA_EXTENSION
        );
        boolean success = false;
        dataIn = EndiannessReverserUtil.openInput(readState.directory, dataFileName, readState.context);
        try {

            CodecUtil.checkIndexHeader(
                dataIn,
                Lucene60MetadataOnlyPointsFormat.DATA_CODEC_NAME,
                Lucene60MetadataOnlyPointsFormat.DATA_VERSION_START,
                Lucene60MetadataOnlyPointsFormat.DATA_VERSION_START,
                readState.segmentInfo.getId(),
                readState.segmentSuffix
            );

            // NOTE: data file is too costly to verify checksum against all the bytes on open,
            // but for now we at least verify proper structure of the checksum footer: which looks
            // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
            // such as file truncation.
            CodecUtil.retrieveChecksum(dataIn);

            for (Map.Entry<Integer, Long> ent : fieldToFileOffset.entrySet()) {
                int fieldNumber = ent.getKey();
                long fp = ent.getValue();
                dataIn.seek(fp);
                PointValues reader = new MetadataOnlyBKDReader(dataIn);
                readers.put(fieldNumber, reader);
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

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(dataIn);
    }

    @Override
    public void close() throws IOException {
        dataIn.close();
        // Free up heap:
        readers.clear();
    }
}
