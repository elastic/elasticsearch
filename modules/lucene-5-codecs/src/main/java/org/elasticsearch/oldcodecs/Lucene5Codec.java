/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.oldcodecs;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50CompoundFormat;
import org.apache.lucene.codecs.lucene50.Lucene50FieldInfosFormat;
import org.apache.lucene.codecs.lucene50.Lucene50LiveDocsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.codecs.lucene87.Lucene87Codec;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

// class currently not used
// this is mainly to show that we don't always need class wrapping
public class Lucene5Codec extends FilterCodec {

    public Lucene5Codec() {
        super("Lucene5Codec", new Lucene87Codec());
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return new Lucene50StoredFieldsFormat();
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        final FieldInfosFormat wrapped = new Lucene50FieldInfosFormat();
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
            fieldInfoCopy.add(new FieldInfo(fieldInfo.name, fieldInfo.number,
                false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1,
                fieldInfo.attributes(), 0, 0, 0,
                fieldInfo.isSoftDeletesField()));
        }
        FieldInfos newFieldInfos = new FieldInfos(fieldInfoCopy.toArray(new FieldInfo[0]));
        return newFieldInfos;
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return new Lucene50SegmentInfoFormat();
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
        return new Lucene50LiveDocsFormat();
    }

    @Override
    public CompoundFormat compoundFormat() {
        return new Lucene50CompoundFormat();
    }

    public static class Lucene50SegmentInfoFormat extends SegmentInfoFormat {

        /**
         * Sole constructor.
         */
        public Lucene50SegmentInfoFormat() {
        }

        @Override
        public SegmentInfo read(Directory dir, String segment,
                                byte[] segmentID, IOContext context) throws IOException {
            final String fileName = IndexFileNames.segmentFileName(segment, "", SI_EXTENSION);
            try (ChecksumIndexInput input = dir.openChecksumInput(fileName, context)) {
                Throwable priorE = null;
                SegmentInfo si = null;
                try {
                    int format = CodecUtil.checkIndexHeader(input, CODEC_NAME,
                        VERSION_START,
                        VERSION_CURRENT,
                        segmentID, "");
                    Version version = Version.fromBits(input.readInt(), input.readInt(), input.readInt());

                    final int docCount = input.readInt();
                    if (docCount < 0) {
                        throw new CorruptIndexException("invalid docCount: " + docCount, input);
                    }
                    final boolean isCompoundFile = input.readByte() == org.apache.lucene5_shaded.index.SegmentInfo.YES;

                    final Map<String, String> diagnostics;
                    final Set<String> files;
                    final Map<String, String> attributes;

                    if (format >= VERSION_SAFE_MAPS) {
                        diagnostics = input.readMapOfStrings();
                        files = input.readSetOfStrings();
                        attributes = input.readMapOfStrings();
                    } else {
                        diagnostics = Collections.unmodifiableMap(input.readMapOfStrings());
                        files = Collections.unmodifiableSet(input.readSetOfStrings());
                        attributes = Collections.unmodifiableMap(input.readMapOfStrings());
                    }

                    // Rewrite version so that it passes checks
                    // TODO: what to do about this?
                    version = Version.LATEST;

                    si = new SegmentInfo(dir, version, version, segment, docCount, isCompoundFile, null, diagnostics, segmentID,
                        attributes, null);
                    si.setFiles(files);
                } catch (Throwable exception) {
                    priorE = exception;
                } finally {
                    CodecUtil.checkFooter(input, priorE);
                }
                return si;
            }
        }

        @Override
        public void write(Directory dir, SegmentInfo si, IOContext ioContext) throws IOException {
            final String fileName = IndexFileNames.segmentFileName(si.name, "", SI_EXTENSION);

            try (IndexOutput output = dir.createOutput(fileName, ioContext)) {
                // Only add the file once we've successfully created it, else IFD assert can trip:
                si.addFile(fileName);
                CodecUtil.writeIndexHeader(output,
                    CODEC_NAME,
                    VERSION_CURRENT,
                    si.getId(),
                    "");
                Version version = si.getVersion();
                if (version.major < 5) {
                    throw new IllegalArgumentException("invalid major version: should be >= 5 but got: " + version.major + " segment=" +
                        si);
                }
                // Write the Lucene version that created this segment, since 3.1
                output.writeInt(version.major);
                output.writeInt(version.minor);
                output.writeInt(version.bugfix);
                assert version.prerelease == 0;
                output.writeInt(si.maxDoc());

                output.writeByte((byte) (si.getUseCompoundFile() ? org.apache.lucene5_shaded.index.SegmentInfo.YES :
                    org.apache.lucene5_shaded.index.SegmentInfo.NO));
                output.writeMapOfStrings(si.getDiagnostics());
                Set<String> files = si.files();
                for (String file : files) {
                    if (IndexFileNames.parseSegmentName(file).equals(si.name) == false) {
                        throw new IllegalArgumentException("invalid files: expected segment=" + si.name + ", got=" + files);
                    }
                }
                output.writeSetOfStrings(files);
                output.writeMapOfStrings(si.getAttributes());
                CodecUtil.writeFooter(output);
            }
        }

        /**
         * File extension used to store {@link org.apache.lucene5_shaded.index.SegmentInfo}.
         */
        public static final String SI_EXTENSION = "si";
        static final String CODEC_NAME = "Lucene50SegmentInfo";
        static final int VERSION_START = 0;
        static final int VERSION_SAFE_MAPS = 1;
        static final int VERSION_CURRENT = VERSION_SAFE_MAPS;
    }
}
