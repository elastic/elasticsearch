/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service;

import org.elasticsearch.watcher.actions.email.service.support.BodyPartSource;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.xcontent.XContentBuilder;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.MessagingException;
import javax.mail.Part;
import javax.mail.internet.MimeBodyPart;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 *
 */
public abstract class Inline extends BodyPartSource {

    protected Inline(String id, String name, String contentType) {
        super(id, name, contentType);
    }

    public abstract String type();

    @Override
    public final MimeBodyPart bodyPart() throws MessagingException {
        MimeBodyPart part = new MimeBodyPart();
        part.setDisposition(Part.INLINE);
        part.setContentID(id);
        part.setFileName(name);
        writeTo(part);
        return part;
    }

    /**
     * intentionally not emitting path as it may come as an information leak
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .field("type", type())
                .field("id", id)
                .field("name", name)
                .field("content_type", contentType)
                .endObject();
    }

    protected abstract void writeTo(MimeBodyPart part) throws MessagingException;

    public static class File extends Inline {

        static final String TYPE = "file";

        private final Path path;
        private DataSource dataSource;

        public File(String id, Path path) {
            this(id, path.getFileName().toString(), path);
        }

        public File(String id, String name, Path path) {
            this(id, name, path, fileTypeMap.getContentType(path.toFile()));
        }

        public File(String id, String name, Path path, String contentType) {
            super(id, name, contentType);
            this.path = path;
            this.dataSource = new FileDataSource(path.toFile());
        }

        public Path path() {
            return path;
        }

        public String type() {
            return TYPE;
        }

        @Override
        public void writeTo(MimeBodyPart part) throws MessagingException {
            part.setDataHandler(new DataHandler(dataSource, contentType));
        }
    }

    public static class Stream extends Inline {

        static final String TYPE = "stream";

        private final Provider<InputStream> source;

        public Stream(String id, String name, Provider<InputStream> source) {
            this(id, name, fileTypeMap.getContentType(name), source);
        }

        public Stream(String id, String name, String contentType, Provider<InputStream> source) {
            super(id, name, contentType);
            this.source = source;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        protected void writeTo(MimeBodyPart part) throws MessagingException {
            DataSource ds = new StreamDataSource(name, contentType, source);
            DataHandler dh = new DataHandler(ds);
            part.setDataHandler(dh);
        }

        static class StreamDataSource implements DataSource {

            private final String name;
            private final String contentType;
            private final Provider<InputStream> source;

            public StreamDataSource(String name, String contentType, Provider<InputStream> source) {
                this.name = name;
                this.contentType = contentType;
                this.source = source;
            }

            @Override
            public InputStream getInputStream() throws IOException {
                return source.get();
            }

            @Override
            public OutputStream getOutputStream() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getContentType() {
                return contentType;
            }

            @Override
            public String getName() {
                return name;
            }
        }
    }

    public static class Bytes extends Stream {

        public Bytes(String id, String name, String contentType, byte[] bytes) {
            super(id, name, contentType, new BytesStreamProvider(bytes));
        }

        public Bytes(String id, String name, String contentType, BytesReference bytes) {
            super(id, name, contentType, new BytesStreamProvider(bytes));
        }

        static class BytesStreamProvider implements Provider<InputStream> {

            private final BytesReference bytes;

            public BytesStreamProvider(byte[] bytes) {
                this(new BytesArray(bytes));
            }

            public BytesStreamProvider(BytesReference bytes) {
                this.bytes = bytes;
            }

            @Override
            public InputStream get() {
                return new ByteArrayInputStream(bytes.array(), bytes.arrayOffset(), bytes.length());
            }
        }
    }
}
