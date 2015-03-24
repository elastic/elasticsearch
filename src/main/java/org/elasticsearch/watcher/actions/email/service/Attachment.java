/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service;

import org.elasticsearch.watcher.actions.email.service.support.BodyPartSource;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.MessagingException;
import javax.mail.Part;
import javax.mail.internet.MimeBodyPart;
import javax.mail.util.ByteArrayDataSource;
import java.io.IOException;
import java.nio.file.Path;

/**
*
*/
public abstract class Attachment extends BodyPartSource {

    protected Attachment(String id, String name, String contentType) {
        super(id, name, contentType);
    }

    @Override
    public final MimeBodyPart bodyPart() throws MessagingException {
        MimeBodyPart part = new MimeBodyPart();
        part.setContentID(id);
        part.setFileName(name);
        part.setDisposition(Part.ATTACHMENT);
        writeTo(part);
        return part;
    }

    public abstract String type();

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

    public static class File extends Attachment {

        static final String TYPE = "file";

        private final Path path;
        private final DataSource dataSource;

        public File(String id, Path path) {
            this(id, path.getFileName().toString(), path);
        }

        public File(String id, Path path, String contentType) {
            this(id, path.getFileName().toString(), path, contentType);
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
            part.setDataHandler(new DataHandler(dataSource));
        }
    }

    public static class Bytes extends Attachment {

        static final String TYPE = "bytes";

        private final byte[] bytes;

        public Bytes(String id, byte[] bytes, String contentType) {
            this(id, id, bytes, contentType);
        }

        public Bytes(String id, String name, byte[] bytes) {
            this(id, name, bytes, fileTypeMap.getContentType(name));
        }

        public Bytes(String id, String name, byte[] bytes, String contentType) {
            super(id, name, contentType);
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

    public static class XContent extends Bytes {

        protected XContent(String id, ToXContent content, XContentType type) {
            this(id, id, content, type);
        }

        protected XContent(String id, String name, ToXContent content, XContentType type) {
            super(id, name, bytes(name, content, type), mimeType(type));
        }

        static String mimeType(XContentType type) {
            switch (type) {
                case JSON:  return "application/json";
                case YAML:  return "application/yaml";
                case SMILE: return "application/smile";
                case CBOR:  return "application/cbor";
                default:
                    throw new EmailException("unsupported xcontent attachment type [" + type.name() + "]");
            }
        }

        static byte[] bytes(String name, ToXContent content, XContentType type) {
            try {
                XContentBuilder builder = XContentBuilder.builder(type.xContent());
                content.toXContent(builder, ToXContent.EMPTY_PARAMS);
                return builder.bytes().toBytes();
            } catch (IOException ioe) {
                throw new EmailException("could not create an xcontent attachment [" + name + "]", ioe);
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
