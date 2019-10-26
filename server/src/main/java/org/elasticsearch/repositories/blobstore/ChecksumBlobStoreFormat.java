/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.CorruptStateException;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Snapshot metadata file format used in v2.0 and above
 */
public final class ChecksumBlobStoreFormat<T extends ToXContent> {

    // Serialization parameters to specify correct context for metadata serialization
    private static final ToXContent.Params SNAPSHOT_ONLY_FORMAT_PARAMS;

    static {
        Map<String, String> snapshotOnlyParams = new HashMap<>();
        // when metadata is serialized certain elements of the metadata shouldn't be included into snapshot
        // exclusion of these elements is done by setting MetaData.CONTEXT_MODE_PARAM to MetaData.CONTEXT_MODE_SNAPSHOT
        snapshotOnlyParams.put(MetaData.CONTEXT_MODE_PARAM, MetaData.CONTEXT_MODE_SNAPSHOT);
        // serialize SnapshotInfo using the SNAPSHOT mode
        snapshotOnlyParams.put(SnapshotInfo.CONTEXT_MODE_PARAM, SnapshotInfo.CONTEXT_MODE_SNAPSHOT);
        SNAPSHOT_ONLY_FORMAT_PARAMS = new ToXContent.MapParams(snapshotOnlyParams);
    }

    // The format version
    public static final int VERSION = 1;

    private static final int BUFFER_SIZE = 4096;

    private final boolean compress;

    private final String codec;

    private final String blobNameFormat;

    private final CheckedFunction<XContentParser, T, IOException> reader;

    private final NamedXContentRegistry namedXContentRegistry;

    /**
     * @param codec          codec name
     * @param blobNameFormat format of the blobname in {@link String#format} format
     * @param reader         prototype object that can deserialize T from XContent
     * @param compress       true if the content should be compressed
     */
    public ChecksumBlobStoreFormat(String codec, String blobNameFormat, CheckedFunction<XContentParser, T, IOException> reader,
                                   NamedXContentRegistry namedXContentRegistry, boolean compress) {
        this.reader = reader;
        this.blobNameFormat = blobNameFormat;
        this.namedXContentRegistry = namedXContentRegistry;
        this.compress = compress;
        this.codec = codec;
    }

    /**
     * Reads and parses the blob with given name, applying name translation using the {link #blobName} method
     *
     * @param blobContainer blob container
     * @param name          name to be translated into
     * @return parsed blob object
     */
    public T read(BlobContainer blobContainer, String name) throws IOException {
        String blobName = blobName(name);
        return readBlob(blobContainer, blobName);
    }

    /**
     * Deletes obj in the blob container
     */
    public void delete(BlobContainer blobContainer, String name) throws IOException {
        blobContainer.deleteBlob(blobName(name));
    }

    public String blobName(String name) {
        return String.format(Locale.ROOT, blobNameFormat, name);
    }

    /**
     * Reads blob with specified name without resolving the blobName using using {@link #blobName} method.
     *
     * @param blobContainer blob container
     * @param blobName blob name
     */
    public T readBlob(BlobContainer blobContainer, String blobName) throws IOException {
        final BytesReference bytes = Streams.readFully(blobContainer.readBlob(blobName));
        final String resourceDesc = "ChecksumBlobStoreFormat.readBlob(blob=\"" + blobName + "\")";
        try (ByteArrayIndexInput indexInput =
                 new ByteArrayIndexInput(resourceDesc, BytesReference.toBytes(bytes))) {
            CodecUtil.checksumEntireFile(indexInput);
            CodecUtil.checkHeader(indexInput, codec, VERSION, VERSION);
            long filePointer = indexInput.getFilePointer();
            long contentSize = indexInput.length() - CodecUtil.footerLength() - filePointer;
            try (XContentParser parser = XContentHelper.createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE,
                bytes.slice((int) filePointer, (int) contentSize), XContentType.SMILE)) {
                return reader.apply(parser);
            }
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // we trick this into a dedicated exception with the original stacktrace
            throw new CorruptStateException(ex);
        }
    }

    /**
     * Writes blob in atomic manner with resolving the blob name using {@link #blobName} method.
     * <p>
     * The blob will be compressed and checksum will be written if required.
     *
     * Atomic move might be very inefficient on some repositories. It also cannot override existing files.
     *
     * @param obj           object to be serialized
     * @param blobContainer blob container
     * @param name          blob name
     */
    public void writeAtomic(T obj, BlobContainer blobContainer, String name) throws IOException {
        final String blobName = blobName(name);
        writeTo(obj, blobName, bytesArray -> {
            try (InputStream stream = bytesArray.streamInput()) {
                blobContainer.writeBlobAtomic(blobName, stream, bytesArray.length(), true);
            }
        });
    }

    /**
     * Writes blob with resolving the blob name using {@link #blobName} method.
     * <p>
     * The blob will be compressed and checksum will be written if required.
     *
     * @param obj                 object to be serialized
     * @param blobContainer       blob container
     * @param name                blob name
     * @param failIfAlreadyExists Whether to fail if the blob already exists
     */
    public void write(T obj, BlobContainer blobContainer, String name, boolean failIfAlreadyExists) throws IOException {
        final String blobName = blobName(name);
        writeTo(obj, blobName, bytesArray -> {
            try (InputStream stream = bytesArray.streamInput()) {
                blobContainer.writeBlob(blobName, stream, bytesArray.length(), failIfAlreadyExists);
            }
        });
    }

    private void writeTo(final T obj, final String blobName, final CheckedConsumer<BytesArray, IOException> consumer) throws IOException {
        final BytesReference bytes;
        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
            if (compress) {
                try (StreamOutput compressedStreamOutput = CompressorFactory.COMPRESSOR.streamOutput(bytesStreamOutput)) {
                    write(obj, compressedStreamOutput);
                }
            } else {
                write(obj, bytesStreamOutput);
            }
            bytes = bytesStreamOutput.bytes();
        }
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            final String resourceDesc = "ChecksumBlobStoreFormat.writeBlob(blob=\"" + blobName + "\")";
            try (OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput(resourceDesc, blobName, outputStream, BUFFER_SIZE)) {
                CodecUtil.writeHeader(indexOutput, codec, VERSION);
                try (OutputStream indexOutputOutputStream = new IndexOutputOutputStream(indexOutput) {
                    @Override
                    public void close() {
                        // this is important since some of the XContentBuilders write bytes on close.
                        // in order to write the footer we need to prevent closing the actual index input.
                    }
                }) {
                    bytes.writeTo(indexOutputOutputStream);
                }
                CodecUtil.writeFooter(indexOutput);
            }
            consumer.accept(new BytesArray(outputStream.toByteArray()));
        }
    }

    private void write(T obj, StreamOutput streamOutput) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.SMILE, streamOutput)) {
            builder.startObject();
            obj.toXContent(builder, SNAPSHOT_ONLY_FORMAT_PARAMS);
            builder.endObject();
        }
    }
}
