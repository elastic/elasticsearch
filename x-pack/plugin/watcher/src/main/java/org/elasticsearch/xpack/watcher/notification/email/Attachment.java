/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.injection.guice.Provider;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.watcher.notification.email.support.BodyPartSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.MessagingException;
import javax.mail.internet.MimeBodyPart;
import javax.mail.util.ByteArrayDataSource;

import static javax.mail.Part.ATTACHMENT;
import static javax.mail.Part.INLINE;

public abstract class Attachment extends BodyPartSource {

    private final boolean inline;
    private final Set<String> warnings;

    protected Attachment(String id, String name, String contentType, boolean inline) {
        this(id, name, contentType, inline, Collections.emptySet());
    }

    protected Attachment(String id, String name, String contentType, boolean inline, Set<String> warnings) {
        super(id, name, contentType);
        this.inline = inline;
        assert warnings != null;
        this.warnings = warnings;
    }

    @Override
    public final MimeBodyPart bodyPart() throws MessagingException {
        MimeBodyPart part = new MimeBodyPart();
        part.setContentID(id);
        part.setFileName(name);
        part.setDisposition(inline ? INLINE : ATTACHMENT);
        writeTo(part);
        return part;
    }

    public abstract String type();

    public boolean isInline() {
        return inline;
    }

    public Set<String> getWarnings() {
        return warnings;
    }

    /**
     * intentionally not emitting path as it may come as an information leak
     */
    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("type", type())
            .field("id", id)
            .field("name", name)
            .field("content_type", contentType)
            .endObject();
    }

    protected abstract void writeTo(MimeBodyPart part) throws MessagingException;

    public static class File extends Attachment {

        static final String TYPE = "file";

        private final Path path;
        private final DataSource dataSource;

        public File(String id, Path path, boolean inline) {
            this(id, path.getFileName().toString(), path, inline);
        }

        public File(String id, Path path, String contentType, boolean inline) {
            this(id, path.getFileName().toString(), path, contentType, inline);
        }

        @SuppressForbidden(reason = "uses toFile")
        public File(String id, String name, Path path, boolean inline) {
            this(id, name, path, fileTypeMap.getContentType(path.toFile()), inline);
        }

        @SuppressForbidden(reason = "uses toFile")
        public File(String id, String name, Path path, String contentType, boolean inline) {
            super(id, name, contentType, inline);
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
            part.setDataHandler(new DataHandler(dataSource));
        }
    }

    public static class Bytes extends Attachment {

        static final String TYPE = "bytes";

        private final byte[] bytes;

        public Bytes(String id, byte[] bytes, String contentType, boolean inline) {
            this(id, id, bytes, contentType, inline, Collections.emptySet());
        }

        public Bytes(String id, String name, byte[] bytes, boolean inline) {
            this(id, name, bytes, fileTypeMap.getContentType(name), inline, Collections.emptySet());
        }

        public Bytes(String id, String name, byte[] bytes, String contentType, boolean inline, Set<String> warnings) {
            super(id, name, contentType, inline, warnings);
            this.bytes = bytes;
        }

        public String type() {
            return TYPE;
        }

        public byte[] bytes() {
            return bytes;
        }

        @Override
        public void writeTo(MimeBodyPart part) throws MessagingException {
            DataSource dataSource = new ByteArrayDataSource(bytes, contentType);
            DataHandler handler = new DataHandler(dataSource);
            part.setDataHandler(handler);
        }
    }

    public static class Stream extends Attachment {

        static final String TYPE = "stream";

        private final Provider<InputStream> source;

        public Stream(String id, String name, boolean inline, Provider<InputStream> source) {
            this(id, name, fileTypeMap.getContentType(name), inline, source);
        }

        public Stream(String id, String name, String contentType, boolean inline, Provider<InputStream> source) {
            super(id, name, contentType, inline);
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

            StreamDataSource(String name, String contentType, Provider<InputStream> source) {
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

    public static class XContent extends Bytes {

        protected XContent(String id, ToXContent content, XContentType type) {
            this(id, id, content, type);
        }

        protected XContent(String id, String name, ToXContent content, XContentType type) {
            super(id, name, bytes(name, content, type), mimeType(type), false, Collections.emptySet());
        }

        static String mimeType(XContentType type) {
            return switch (type) {
                case JSON -> "application/json";
                case YAML -> "application/yaml";
                case SMILE -> "application/smile";
                case CBOR -> "application/cbor";
                default -> throw new IllegalArgumentException("unsupported xcontent attachment type [" + type.name() + "]");
            };
        }

        static byte[] bytes(String name, ToXContent content, XContentType type) {
            try {
                XContentBuilder builder = XContentBuilder.builder(type.xContent()).prettyPrint();
                content.toXContent(builder, ToXContent.EMPTY_PARAMS);
                return BytesReference.toBytes(BytesReference.bytes(builder));
            } catch (IOException ioe) {
                throw new ElasticsearchException("could not create an xcontent attachment [" + name + "]", ioe);
            }
        }

        public static class Yaml extends XContent {

            public Yaml(String id, ToXContent content) {
                super(id, content, XContentType.YAML);
            }

            public Yaml(String id, String name, ToXContent content) {
                super(id, name, content, XContentType.YAML);
            }

            @Override
            public String type() {
                return "yaml";
            }
        }

        public static class Json extends XContent {

            public Json(String id, ToXContent content) {
                super(id, content, XContentType.JSON);
            }

            public Json(String id, String name, ToXContent content) {
                super(id, name, content, XContentType.JSON);
            }

            @Override
            public String type() {
                return "json";
            }
        }
    }
}
