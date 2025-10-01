/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.index;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.ingest.ESONFlat;
import org.elasticsearch.ingest.ESONIndexed;
import org.elasticsearch.ingest.ESONSource;
import org.elasticsearch.ingest.ESONXContentSerializer;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

/**
 * The {@link IndexSource} is a class which holds the source for an {@link IndexRequest}. This class is designed to encapsulate the source
 * lifecycle and allow the bytes to be accessed and released when reserialized.
 */
public class IndexSource implements Writeable, Releasable {

    public static final TransportVersion STRUCTURED_SOURCE = TransportVersion.fromName("structured_source");

    private XContentType contentType;
    private BytesReference source;
    private int bytesSourceSize = 0;
    private ESONFlat structuredSource;
    private boolean isClosed = false;

    public IndexSource() {}

    public IndexSource(BytesReference source) {
        this(XContentHelper.xContentType(source), source);
    }

    public IndexSource(XContentType contentType, BytesReference source) {
        this.contentType = contentType;
        this.source = source;
        this.bytesSourceSize = source.length();
    }

    public IndexSource(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            // faster than StreamInput::readEnum, do not replace we read a lot of these instances at times
            contentType = XContentType.ofOrdinal(in.readByte());
        } else {
            contentType = null;
        }

        if (in.getTransportVersion().supports(STRUCTURED_SOURCE)) {
            if (in.readBoolean()) {
                bytesSourceSize = in.readVInt();
                structuredSource = ESONFlat.readFrom(in);
                source = null;
            } else {
                source = in.readBytesReference();
                bytesSourceSize = source.length();
                structuredSource = null;
            }
        } else {
            source = in.readBytesReference();
            bytesSourceSize = source.length();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert isClosed == false;
        if (contentType != null) {
            out.writeBoolean(true);
            XContentHelper.writeTo(out, contentType);
        } else {
            out.writeBoolean(false);
        }
        if (out.getTransportVersion().supports(STRUCTURED_SOURCE)) {
            if (isStructured()) {
                out.writeBoolean(true);
                out.writeVInt(bytesSourceSize);
                structuredSource.writeTo(out);
            } else {
                out.writeBoolean(false);
                out.writeBytesReference(bytes());
            }
        } else {
            out.writeBytesReference(source);
        }
    }

    public XContentType contentType() {
        assert isClosed == false;
        return contentType;
    }

    public BytesReference bytes() {
        assert isClosed == false;
        if (source == null && structuredSource != null) {
            try (XContentBuilder builder = XContentFactory.contentBuilder(contentType)) {
                ESONXContentSerializer.flattenToXContent(structuredSource, builder, ToXContent.EMPTY_PARAMS);
                source = BytesReference.bytes(builder);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return source;
    }

    public boolean hasSource() {
        assert isClosed == false;
        return source != null || structuredSource != null;
    }

    public int byteLength() {
        assert isClosed == false;
        return source == null ? 0 : source.length();
    }

    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() {
        assert isClosed == false;
        isClosed = true;
        source = null;
        contentType = null;
    }

    public boolean isSourceEmpty() {
        // TODO: check this logic. What does an empty source get converted into?
        if (structuredSource != null) {
            return false;
        } else {
            return source == null || source.length() == 0;
        }
    }

    public void ensureStructured() {
        if (structuredSource == null) {
            assert source != null;
            ESONSource.Builder builder = new ESONSource.Builder((int) (source.length() * 0.70));
            try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, contentType)) {
                structuredSource = builder.parse(parser).esonFlat();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public ESONFlat structuredSource() {
        if (structuredSource == null) {
            ensureStructured();
        }
        return structuredSource;
    }

    public boolean isStructured() {
        return structuredSource != null;
    }

    public Map<String, Object> sourceAsMap() {
        assert isClosed == false;
        return ESONIndexed.fromFlat(structuredSource());
    }

    @Override
    public boolean equals(Object o) {
        // TODO: Improve
        if (o == null || getClass() != o.getClass()) return false;
        return ((IndexSource) o).bytes().equals(bytes());
    }

    @Override
    public int hashCode() {
        // TODO: Improve
        return bytes().hashCode();
    }

    /**
     * Index the Map in {@link Requests#INDEX_CONTENT_TYPE} format
     *
     * @param source The map to index
     */
    public void source(Map<String, ?> source) throws ElasticsearchGenerationException {
        source(source, Requests.INDEX_CONTENT_TYPE);
    }

    /**
     * Index the Map as the provided content type.
     *
     * @param source The map to index
     */
    public void source(Map<String, ?> source, XContentType contentType) throws ElasticsearchGenerationException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(source);
            source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public void source(Map<String, ?> source, XContentType contentType, boolean ensureNoSelfReferences)
        throws ElasticsearchGenerationException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(source, ensureNoSelfReferences);
            source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the document source to index.
     * <p>
     * Note, its preferable to either set it using {@link #source(org.elasticsearch.xcontent.XContentBuilder)}
     * or using the {@link #source(byte[], XContentType)}.
     */
    public void source(String source, XContentType xContentType) {
        source(new BytesArray(source), xContentType);
    }

    /**
     * Sets the content source to index.
     */
    public void source(XContentBuilder sourceBuilder) {
        source(BytesReference.bytes(sourceBuilder), sourceBuilder.contentType());
    }

    /**
     * Sets the content source to index using the default content type ({@link Requests#INDEX_CONTENT_TYPE})
     * <p>
     * <b>Note: the number of objects passed to this method must be an even
     * number. Also the first argument in each pair (the field name) must have a
     * valid String representation.</b>
     * </p>
     */
    public void source(Object... source) {
        source(Requests.INDEX_CONTENT_TYPE, source);
    }

    /**
     * Sets the content source to index.
     * <p>
     * <b>Note: the number of objects passed to this method as varargs must be an even
     * number. Also the first argument in each pair (the field name) must have a
     * valid String representation.</b>
     * </p>
     */
    public void source(XContentType xContentType, Object... source) {
        source(getXContentBuilder(xContentType, source));
    }

    /**
     * Returns an XContentBuilder for the given xContentType and source array
     * <p>
     * <b>Note: the number of objects passed to this method as varargs must be an even
     * number. Also the first argument in each pair (the field name) must have a
     * valid String representation.</b>
     * </p>
     */
    public static XContentBuilder getXContentBuilder(XContentType xContentType, Object... source) {
        if (source.length % 2 != 0) {
            throw new IllegalArgumentException("The number of object passed must be even but was [" + source.length + "]");
        }
        if (source.length == 2 && source[0] instanceof BytesReference && source[1] instanceof Boolean) {
            throw new IllegalArgumentException(
                "you are using the removed method for source with bytes and unsafe flag, the unsafe flag"
                    + " was removed, please just use source(BytesReference)"
            );
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
            builder.startObject();
            // This for loop increments by 2 because the source array contains adjacent key/value pairs:
            for (int i = 0; i < source.length; i = i + 2) {
                String field = source[i].toString();
                Object value = source[i + 1];
                builder.field(field, value);
            }
            builder.endObject();
            return builder;
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate", e);
        }
    }

    public void source(ReleasableBytesReference source, XContentType contentType) {
        setSource(source, contentType);
    }

    /**
     * Sets the document to index in bytes form.
     */
    public void source(BytesReference source, XContentType contentType) {
        setSource(source, contentType);
    }

    /**
     * Sets the document to index in bytes form.
     */
    public void source(byte[] source, XContentType contentType) {
        source(source, 0, source.length, contentType);
    }

    /**
     * Sets the document to index in bytes form (assumed to be safe to be used from different
     * threads).
     *
     * @param source The source to index
     * @param offset The offset in the byte array
     * @param length The length of the data
     */
    public void source(byte[] source, int offset, int length, XContentType contentType) {
        source(new BytesArray(source, offset, length), contentType);
    }

    public void structuredSource(ESONIndexed.ESONObject esonSource) {
        assert isClosed == false;
        this.structuredSource = ESONIndexed.flatten(esonSource);
        this.source = null;
    }

    private void setSource(BytesReference source, XContentType contentType) {
        assert isClosed == false;
        this.source = source;
        this.bytesSourceSize = source.length();
        this.contentType = contentType;
        this.structuredSource = null;
    }
}
