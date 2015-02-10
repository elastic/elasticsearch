/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions.email.service;

import org.elasticsearch.alerts.actions.email.service.support.BodyPartSource;
import org.elasticsearch.common.xcontent.XContentBuilder;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.MessagingException;
import javax.mail.Part;
import javax.mail.internet.MimeBodyPart;
import java.io.IOException;
import java.nio.file.Path;

/**
 *
 */
public abstract class Inline extends BodyPartSource {

    public Inline(String id) {
        super(id);
    }

    public Inline(String id, String name) {
        super(id, name);
    }

    public Inline(String id, String name, String description) {
        super(id, name, description);
    }

    @Override
    public final MimeBodyPart bodyPart() throws MessagingException {
        MimeBodyPart part = new MimeBodyPart();
        part.setDisposition(Part.INLINE);
        writeTo(part);
        return part;
    }

    protected abstract void writeTo(MimeBodyPart part) throws MessagingException;

    public static class File extends Inline {

        static final String TYPE = "file";

        private final Path path;
        private DataSource dataSource;
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
}
