/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.apache.lucene.codecs.lucene50;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.common.collect.Tuple;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CompoundReaderUtils {

    private CompoundReaderUtils() {}

    public static Map<String, Map<String, Tuple<Long, Long>>> extractCompoundFiles(Directory directory) throws IOException {
        final Map<String, Map<String, Tuple<Long, Long>>> compoundFiles = new HashMap<>();
        final SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
        for (SegmentCommitInfo segmentCommitInfo : segmentInfos) {
            final SegmentInfo segmentInfo = segmentCommitInfo.info;
            if (segmentInfo.getUseCompoundFile()) {
                final Codec codec = segmentInfo.getCodec();
                try (CompoundDirectory compoundDir = codec.compoundFormat().getCompoundReader(directory, segmentInfo, IOContext.DEFAULT)) {
                    String className = compoundDir.getClass().getName();
                    switch (className) {
                        case "org.apache.lucene.codecs.lucene50.Lucene50CompoundReader":
                            compoundFiles.put(segmentInfo.name, readEntries(directory, segmentCommitInfo.info));
                            break;
                        default:
                            assert false : "please implement readEntries() for this format of compound files: " + className;
                            throw new IllegalStateException("This format of compound files is not supported: " + className);
                    }
                }
            }
        }
        return Collections.unmodifiableMap(compoundFiles);
    }

    private static Map<String, Tuple<Long, Long>> readEntries(Directory directory, SegmentInfo segmentInfo) throws IOException {
        final String entriesFileName = IndexFileNames.segmentFileName(segmentInfo.name, "", Lucene50CompoundFormat.ENTRIES_EXTENSION);
        try (ChecksumIndexInput entriesStream = directory.openChecksumInput(entriesFileName, IOContext.READONCE)) {
            Map<String, Tuple<Long, Long>> mapping = new HashMap<>();
            Throwable trowable = null;
            try {
                CodecUtil.checkIndexHeader(
                    entriesStream,
                    Lucene50CompoundFormat.ENTRY_CODEC,
                    Lucene50CompoundFormat.VERSION_START,
                    Lucene50CompoundFormat.VERSION_CURRENT,
                    segmentInfo.getId(),
                    ""
                );

                final int numEntries = entriesStream.readVInt();
                final Set<String> seen = new HashSet<>(numEntries);
                for (int i = 0; i < numEntries; i++) {
                    final String id = entriesStream.readString();
                    if (seen.add(id) == false) {
                        throw new CorruptIndexException("Duplicate cfs entry id=" + id + " in CFS ", entriesStream);
                    }
                    long offset = entriesStream.readLong();
                    long length = entriesStream.readLong();
                    mapping.put(id, Tuple.tuple(offset, length));
                }
                assert mapping.size() == numEntries;
            } catch (Throwable exception) {
                trowable = exception;
            } finally {
                CodecUtil.checkFooter(entriesStream, trowable);
            }
            return Collections.unmodifiableMap(mapping);
        }
    }
}
