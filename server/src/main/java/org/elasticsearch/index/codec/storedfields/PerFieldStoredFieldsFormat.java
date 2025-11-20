/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.storedfields;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.bloomfilter.ES93BloomFilterStoredFieldsFormat;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Enables per field stored fields format support.
 *
 * <p> This class uses SPI to resolve format names.</p>
 *
 * <p> Files written by each stored fields format should use different file fileExtensions, this is enforced during the writer creation.</p>
 */
public abstract class PerFieldStoredFieldsFormat extends StoredFieldsFormat {
    public static final String STORED_FIELD_FORMAT_ATTRIBUTE_KEY = "stored_field_format";

    // We don't need to add support for loading stored fields format through SPI since we control
    // all the implementations, thus a simple static map is enough to load them on read time.
    private static final Map<String, ESStoredFieldsFormat> AVAILABLE_FORMATS = Map.of(
        ESLucene90StoredFieldsFormat.FORMAT_NAME,
        new ESLucene90StoredFieldsFormat(),
        ESZstd814StoredFieldsFormat.FORMAT_NAME,
        new ESZstd814StoredFieldsFormat(),
        ES93BloomFilterStoredFieldsFormat.FORMAT_NAME,
        new ES93BloomFilterStoredFieldsFormat()
    );

    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
        return new PerFieldStoredFieldsReader(directory, si, fn, context);
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
        return new PerFieldStoredFieldsWriter(directory, si, context);
    }

    protected abstract ESStoredFieldsFormat getStoredFieldsFormatForField(String field);

    class PerFieldStoredFieldsWriter extends StoredFieldsWriter {
        private final IntObjectHashMap<StoredFieldsWriterAndMetadata> fields = new IntObjectHashMap<>();
        private final Map<StoredFieldsFormat, StoredFieldsWriterAndMetadata> formatWriters = new HashMap<>();

        private final Directory directory;
        private final SegmentInfo si;
        private final IOContext context;

        private int numStartedDocs = 0;
        private int numFinishedDocs = 0;

        PerFieldStoredFieldsWriter(Directory directory, SegmentInfo si, IOContext context) {
            this.directory = directory;
            this.si = si;
            this.context = context;
        }

        @Override
        public void startDocument() throws IOException {
            for (var writerAndExtensions : formatWriters.values()) {
                writerAndExtensions.writer().startDocument();
            }
            numStartedDocs++;
        }

        @Override
        public void finishDocument() throws IOException {
            for (var writerAndExtensions : formatWriters.values()) {
                writerAndExtensions.writer().finishDocument();
            }
            numFinishedDocs++;
        }

        @Override
        public void writeField(FieldInfo info, int value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, long value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, float value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, double value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, BytesRef value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, String value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void finish(int numDocs) throws IOException {
            for (var writerAndExtensions : formatWriters.values()) {
                writerAndExtensions.writer().finish(numDocs);
            }
        }

        @Override
        public int merge(MergeState mergeState) throws IOException {
            Map<String, StoredFieldsWriter> formatWriters = new HashMap<>();
            for (FieldInfo mergeFieldInfo : mergeState.mergeFieldInfos) {
                var writerAndMetadata = getWriterAndMetadataForField(mergeFieldInfo);
                formatWriters.put(writerAndMetadata.formatName(), writerAndMetadata.writer());
            }

            var totalDocs = 0;
            for (Map.Entry<String, StoredFieldsWriter> formatNameAndWriter : formatWriters.entrySet()) {
                final String writerFormatName = formatNameAndWriter.getKey();
                final StoredFieldsWriter formatWriter = formatNameAndWriter.getValue();
                StoredFieldsReader[] updatedReaders = new StoredFieldsReader[mergeState.storedFieldsReaders.length];
                for (int i = 0; i < mergeState.storedFieldsReaders.length; i++) {
                    final StoredFieldsReader storedFieldsReader = mergeState.storedFieldsReaders[i];

                    // We need to unwrap the stored field readers belonging to a PerFieldStoredFieldsFormat,
                    // otherwise, downstream formats won't be able to perform certain optimizations when
                    // they try to merge segments as they expect an instance of the actual Reader in their checks
                    // (i.e. Lucene90CompressingStoredFieldsReader would do chunk merging for instances of the same class)
                    if (storedFieldsReader instanceof PerFieldStoredFieldsReader reader) {
                        final var formatStoredFieldsReader = reader.getFormatToStoredFieldReaders().get(writerFormatName);
                        // In case that we're dealing with a previous format, we just fall back to the slow path
                        updatedReaders[i] = Objects.requireNonNullElse(formatStoredFieldsReader, storedFieldsReader);
                    } else {
                        updatedReaders[i] = storedFieldsReader;
                    }
                }

                var updatedMergeState = new MergeState(
                    mergeState.docMaps,
                    mergeState.segmentInfo,
                    mergeState.mergeFieldInfos,
                    updatedReaders,
                    mergeState.termVectorsReaders,
                    mergeState.normsProducers,
                    mergeState.docValuesProducers,
                    mergeState.fieldInfos,
                    mergeState.liveDocs,
                    mergeState.fieldsProducers,
                    mergeState.pointsReaders,
                    mergeState.knnVectorsReaders,
                    mergeState.maxDocs,
                    mergeState.infoStream,
                    mergeState.intraMergeTaskExecutor,
                    mergeState.needsIndexSort
                );

                totalDocs += formatWriter.merge(updatedMergeState);
            }
            return totalDocs;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(formatWriters.values());
        }

        @Override
        public long ramBytesUsed() {
            long ramBytesUsed = 0;
            for (var writer : formatWriters.values()) {
                ramBytesUsed += writer.writer().ramBytesUsed();
            }
            return ramBytesUsed;
        }

        private StoredFieldsWriter getWriterForField(FieldInfo field) throws IOException {
            return getWriterAndMetadataForField(field).writer;
        }

        private StoredFieldsWriterAndMetadata getWriterAndMetadataForField(FieldInfo field) throws IOException {
            var writer = fields.get(field.number);
            if (writer != null) {
                return writer;
            }

            var format = getStoredFieldsFormatForField(field.name);

            if (format == null) {
                throw new IllegalStateException("invalid null StoredFieldsFormat for field=\"" + field.name + "\"");
            }

            var formatWriter = formatWriters.get(format);
            if (formatWriter == null) {
                for (StoredFieldsWriterAndMetadata value : formatWriters.values()) {
                    if (Sets.intersection(value.fileExtensions(), format.getFileExtensions()).isEmpty() == false) {
                        throw new IllegalStateException(
                            "File extension conflict for field '"
                                + field.name
                                + "': format "
                                + format.getName()
                                + " has overlapping fileExtensions with existing format"
                        );
                    }
                }
                formatWriter = new StoredFieldsWriterAndMetadata(
                    format.getName(),
                    format.getFileExtensions(),
                    format.fieldsWriter(directory, si, context)
                );

                // Ensure that the doc count is consistent so when #finish is called
                // all formats have a consistent doc count
                for (int i = 0; i < numStartedDocs; i++) {
                    formatWriter.writer().startDocument();
                }
                for (int i = 0; i < numFinishedDocs; i++) {
                    formatWriter.writer().finishDocument();
                }

                var previous = formatWriters.put(format, formatWriter);
                assert previous == null;
            }
            fields.put(field.number, formatWriter);
            field.putAttribute(STORED_FIELD_FORMAT_ATTRIBUTE_KEY, format.getName());

            return formatWriter;
        }
    }

    record StoredFieldsWriterAndMetadata(String formatName, Set<String> fileExtensions, StoredFieldsWriter writer) implements Closeable {
        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    public static class PerFieldStoredFieldsReader extends StoredFieldsReader {
        private final Map<String, StoredFieldsReader> formatToStoredFieldReaders;
        private final Map<String, String> fieldToFormat;

        PerFieldStoredFieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
            HashMap<String, StoredFieldsReader> formatStoredFieldReaders = new HashMap<>();
            HashMap<String, String> fieldToFormat = new HashMap<>();
            boolean success = false;
            try {
                for (FieldInfo fi : fn) {
                    final String formatName = fi.getAttribute(STORED_FIELD_FORMAT_ATTRIBUTE_KEY);
                    // Can be a format name be null if we're reading a segment from this codec?
                    if (formatName != null) {
                        var storedFieldsReader = formatStoredFieldReaders.get(formatName);
                        if (storedFieldsReader == null) {
                            ESStoredFieldsFormat format = AVAILABLE_FORMATS.get(formatName);
                            storedFieldsReader = format.fieldsReader(directory, si, fn, context);
                            var previous = formatStoredFieldReaders.put(formatName, storedFieldsReader);
                            assert previous == null;
                        }
                        fieldToFormat.put(fi.name, formatName);
                    }
                }
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.close(formatStoredFieldReaders.values());
                }
            }
            this.formatToStoredFieldReaders = Collections.unmodifiableMap(formatStoredFieldReaders);
            this.fieldToFormat = Collections.unmodifiableMap(fieldToFormat);
        }

        PerFieldStoredFieldsReader(Map<String, StoredFieldsReader> formatToStoredFieldReaders, Map<String, String> fieldToFormat) {
            this.formatToStoredFieldReaders = Collections.unmodifiableMap(formatToStoredFieldReaders);
            this.fieldToFormat = Collections.unmodifiableMap(fieldToFormat);
        }

        @Override
        public StoredFieldsReader clone() {
            Map<String, StoredFieldsReader> clonedFormats = Maps.newMapWithExpectedSize(formatToStoredFieldReaders.size());
            for (Map.Entry<String, StoredFieldsReader> entry : formatToStoredFieldReaders.entrySet()) {
                clonedFormats.put(entry.getKey(), entry.getValue().clone());
            }
            return new PerFieldStoredFieldsReader(clonedFormats, fieldToFormat);
        }

        @Override
        public StoredFieldsReader getMergeInstance() {
            Map<String, StoredFieldsReader> mergeFormats = Maps.newMapWithExpectedSize(formatToStoredFieldReaders.size());
            for (Map.Entry<String, StoredFieldsReader> entry : formatToStoredFieldReaders.entrySet()) {
                mergeFormats.put(entry.getKey(), entry.getValue().getMergeInstance());
            }
            return new PerFieldStoredFieldsReader(mergeFormats, fieldToFormat);
        }

        @Override
        public void checkIntegrity() throws IOException {
            for (StoredFieldsReader storedFieldsReader : formatToStoredFieldReaders.values()) {
                storedFieldsReader.checkIntegrity();
            }
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(formatToStoredFieldReaders.values());
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            for (StoredFieldsReader storedFieldsReader : formatToStoredFieldReaders.values()) {
                storedFieldsReader.document(docID, visitor);
            }
        }

        @Nullable
        public StoredFieldsReader getReaderForField(String fieldName) {
            String formatName = fieldToFormat.get(fieldName);
            return formatName != null ? formatToStoredFieldReaders.get(formatName) : null;
        }

        private Map<String, StoredFieldsReader> getFormatToStoredFieldReaders() {
            return formatToStoredFieldReaders;
        }
    }
}
