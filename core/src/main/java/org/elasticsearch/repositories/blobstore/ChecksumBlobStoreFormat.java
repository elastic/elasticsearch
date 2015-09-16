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
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.gateway.CorruptStateException;

import java.io.*;
import java.util.Locale;

/**
 * Snapshot metadata file format used in v2.0 and above
 */
public class ChecksumBlobStoreFormat<T extends ToXContent> extends BlobStoreFormat<T> {

    private static final String TEMP_FILE_PREFIX = "pending-";

    private static final XContentType DEFAULT_X_CONTENT_TYPE = XContentType.SMILE;

    // The format version
    public static final int VERSION = 1;

    private static final int BUFFER_SIZE = 4096;

    protected final XContentType xContentType;

    protected final boolean compress;

    private final String codec;

    /**
     * @param codec          codec name
     * @param blobNameFormat format of the blobname in {@link String#format} format
     * @param reader         prototype object that can deserialize T from XContent
     * @param compress       true if the content should be compressed
     * @param xContentType   content type that should be used for write operations
     */
    public ChecksumBlobStoreFormat(String codec, String blobNameFormat, FromXContentBuilder<T> reader, ParseFieldMatcher parseFieldMatcher, boolean compress, XContentType xContentType) {
        super(blobNameFormat, reader, parseFieldMatcher);
        this.xContentType = xContentType;
        this.compress = compress;
        this.codec = codec;
    }

    /**
     * @param codec          codec name
     * @param blobNameFormat format of the blobname in {@link String#format} format
     * @param reader         prototype object that can deserialize T from XContent
     * @param compress       true if the content should be compressed
     */
    public ChecksumBlobStoreFormat(String codec, String blobNameFormat, FromXContentBuilder<T> reader, ParseFieldMatcher parseFieldMatcher, boolean compress) {
        this(codec, blobNameFormat, reader, parseFieldMatcher, compress, DEFAULT_X_CONTENT_TYPE);
    }

    /**
     * Reads blob with specified name without resolving the blobName using using {@link #blobName} method.
     *
     * @param blobContainer blob container
     * @param blobName blob name
     * @return
     * @throws IOException
     */
    public T readBlob(BlobContainer blobContainer, String blobName) throws IOException {
        try (InputStream inputStream = blobContainer.readBlob(blobName)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(inputStream, out);
            final byte[] bytes = out.toByteArray();
            final String resourceDesc = "ChecksumBlobStoreFormat.readBlob(blob=\"" + blobName + "\")";
            try (ByteArrayIndexInput indexInput = new ByteArrayIndexInput(resourceDesc, bytes)) {
                CodecUtil.checksumEntireFile(indexInput);
                CodecUtil.checkHeader(indexInput, codec, VERSION, VERSION);
                long filePointer = indexInput.getFilePointer();
                long contentSize = indexInput.length() - CodecUtil.footerLength() - filePointer;
                BytesReference bytesReference = new BytesArray(bytes, (int) filePointer, (int) contentSize);
                return read(bytesReference);
            } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
                // we trick this into a dedicated exception with the original stacktrace
                throw new CorruptStateException(ex);
            }
        }
    }

    /**
     * Writes blob in atomic manner with resolving the blob name using {@link #blobName} and {@link #tempBlobName} methods.
     * <p/>
     * The blob will be compressed and checksum will be written if required.
     *
     * Atomic move might be very inefficient on some repositories. It also cannot override existing files.
     *
     * @param obj           object to be serialized
     * @param blobContainer blob container
     * @param name          blob name
     * @throws IOException
     */
    public void writeAtomic(T obj, BlobContainer blobContainer, String name) throws IOException {
        String blobName = blobName(name);
        String tempBlobName = tempBlobName(name);
        writeBlob(obj, blobContainer, tempBlobName);
        try {
            blobContainer.move(tempBlobName, blobName);
        } catch (IOException ex) {
            // Move failed - try cleaning up
            blobContainer.deleteBlob(tempBlobName);
            throw ex;
        }
    }

    /**
     * Writes blob with resolving the blob name using {@link #blobName} method.
     * <p/>
     * The blob will be compressed and checksum will be written if required.
     *
     * @param obj           object to be serialized
     * @param blobContainer blob container
     * @param name          blob name
     * @throws IOException
     */
    public void write(T obj, BlobContainer blobContainer, String name) throws IOException {
        String blobName = blobName(name);
        writeBlob(obj, blobContainer, blobName);
    }

    /**
     * Writes blob in atomic manner without resolving the blobName using using {@link #blobName} method.
     * <p/>
     * The blob will be compressed and checksum will be written if required.
     *
     * @param obj           object to be serialized
     * @param blobContainer blob container
     * @param blobName          blob name
     * @throws IOException
     */
    protected void writeBlob(T obj, BlobContainer blobContainer, String blobName) throws IOException {
        BytesReference bytes = write(obj);
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            final String resourceDesc = "ChecksumBlobStoreFormat.writeBlob(blob=\"" + blobName + "\")";
            try (OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput(resourceDesc, byteArrayOutputStream, BUFFER_SIZE)) {
                CodecUtil.writeHeader(indexOutput, codec, VERSION);
                try (OutputStream indexOutputOutputStream = new IndexOutputOutputStream(indexOutput) {
                    @Override
                    public void close() throws IOException {
                        // this is important since some of the XContentBuilders write bytes on close.
                        // in order to write the footer we need to prevent closing the actual index input.
                    } }) {
                    bytes.writeTo(indexOutputOutputStream);
                }
                CodecUtil.writeFooter(indexOutput);
            }
            blobContainer.writeBlob(blobName, new BytesArray(byteArrayOutputStream.toByteArray()));
        }
    }

    /**
     * Returns true if the blob is a leftover temporary blob.
     *
     * The temporary blobs might be left after failed atomic write operation.
     */
    public boolean isTempBlobName(String blobName) {
        return blobName.startsWith(ChecksumBlobStoreFormat.TEMP_FILE_PREFIX);
    }

    protected BytesReference write(T obj) throws IOException {
        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
            if (compress) {
                try (StreamOutput compressedStreamOutput = CompressorFactory.defaultCompressor().streamOutput(bytesStreamOutput)) {
                    write(obj, compressedStreamOutput);
                }
            } else {
                write(obj, bytesStreamOutput);
            }
            return bytesStreamOutput.bytes();
        }
    }

    protected void write(T obj, StreamOutput streamOutput) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(xContentType, streamOutput)) {
            builder.startObject();
            obj.toXContent(builder, SNAPSHOT_ONLY_FORMAT_PARAMS);
            builder.endObject();
        }
    }


    protected String tempBlobName(String name) {
        return TEMP_FILE_PREFIX + String.format(Locale.ROOT, blobNameFormat, name);
    }

}
