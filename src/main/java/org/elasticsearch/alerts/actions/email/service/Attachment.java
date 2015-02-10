/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions.email.service;

import org.elasticsearch.alerts.actions.email.service.support.BodyPartSource;
import org.elasticsearch.common.base.Charsets;
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

    public Attachment(String id) {
        super(id);
    }

    public Attachment(String id, String name) {
        super(id, name);
    }

    public Attachment(String id, String name, String description) {
        super(id, name, description);
    }

    @Override
    public final MimeBodyPart bodyPart() throws MessagingException {
        MimeBodyPart part = new MimeBodyPart();
        part.setContentID(id);
        part.setFileName(name);
        part.setDescription(description, Charsets.UTF_8.name());
        part.setDisposition(Part.ATTACHMENT);
        writeTo(part);
        return part;
    }

    protected abstract void writeTo(MimeBodyPart part) throws MessagingException;

    public static class File extends Attachment {

        static final String TYPE = "file";

        private final Path path;
        private final DataSource dataSource;
        private final String contentType;

        public File(String id, Path path) {
            this(id, path.getFileName().toString(), path);
        }

        public File(String id, Path path, String contentType) {
            this(id, path.getFileName().toString(), path, contentType);
        }

        public File(String id, String name, Path path) {
            this(id, name, name, path);
        }
        public File(String id, String name, Path path, String contentType) {
            this(id, name, name, path, contentType);
        }

        public File(String id, String name, String description, Path path) {
            this(id, name, description, path, null);
        }

        public File(String id, String name, String description, Path path, String contentType) {
            super(id, name, description);
            this.path = path;
            this.dataSource = new FileDataSource(path.toFile());
            this.contentType = contentType;
        }

        public Path path() {
            return path;
        }

        public String type() {
            return TYPE;
        }

        String contentType() {
            return contentType != null ? contentType : dataSource.getContentType();
        }

        @Override
        public void writeTo(MimeBodyPart part) throws MessagingException {
            DataSource dataSource = new FileDataSource(path.toFile());
            DataHandler handler = contentType != null ?
                    new DataHandler(dataSource, contentType) :
                    new DataHandler(dataSource);
            part.setDataHandler(handler);
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
                    .field("description", description)
                    .field("content_type", contentType())
                    .endObject();
        }
    }

    public static class Bytes extends Attachment {

        static final String TYPE = "bytes";

        private final byte[] bytes;
        private final String contentType;

        public Bytes(String id, byte[] bytes, String contentType) {
            this(id, id, bytes, contentType);
        }

        public Bytes(String id, String name, byte[] bytes, String contentType) {
            this(id, name, name, bytes, contentType);
        }

        public Bytes(String id, String name, String description, byte[] bytes, String contentType) {
            super(id, name, description);
            this.bytes = bytes;
            this.contentType = contentType;
        }

        public String type() {
            return TYPE;
        }

        public byte[] bytes() {
            return bytes;
        }

        public String contentType() {
            return contentType;
        }

        @Override
        public void writeTo(MimeBodyPart part) throws MessagingException {
            DataSource dataSource = new ByteArrayDataSource(bytes, contentType);
            DataHandler handler = new DataHandler(dataSource);
            part.setDataHandler(handler);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                    .field("type", type())
                    .field("id", id)
                    .field("name", name)
                    .field("description", description)
                    .field("content_type", contentType)
                    .endObject();
        }
    }

    public static class XContent extends Bytes {

        protected XContent(String id, ToXContent content, XContentType type) {
            this(id, id, content, type);
        }

        protected XContent(String id, String name, ToXContent content, XContentType type) {
            this(id, name, name, content, type);
        }

        protected XContent(String id, String name, String description, ToXContent content, XContentType type) {
            super(id, name, description, bytes(name, content, type), mimeType(type));
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
                return builder.bytes().array();
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

            public Yaml(String id, String name, String description, ToXContent content) {
                super(id, name, description, content, XContentType.YAML);
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

            public Json(String id, String name, String description, ToXContent content) {
                super(id, name, description, content, XContentType.JSON);
            }

            @Override
            public String type() {
                return "json";
            }
        }
    }
}
