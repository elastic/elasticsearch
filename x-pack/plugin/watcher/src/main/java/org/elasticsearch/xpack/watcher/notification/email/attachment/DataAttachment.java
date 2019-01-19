/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.email.attachment;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class DataAttachment implements EmailAttachmentParser.EmailAttachment {

    private final String id;
    private final org.elasticsearch.xpack.watcher.notification.email.DataAttachment dataAttachment;

    public DataAttachment(String id, org.elasticsearch.xpack.watcher.notification.email.DataAttachment dataAttachment) {
        this.id = id;
        this.dataAttachment = dataAttachment;
    }

    public org.elasticsearch.xpack.watcher.notification.email.DataAttachment getDataAttachment() {
        return dataAttachment;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(id).startObject(DataAttachmentParser.TYPE);
        if (dataAttachment == org.elasticsearch.xpack.watcher.notification.email.DataAttachment.YAML) {
            builder.field("format", "yaml");
        } else {
            builder.field("format", "json");
        }
        return builder.endObject().endObject();
    }

    @Override
    public String type() {
        return DataAttachmentParser.TYPE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataAttachment otherDataAttachment = (DataAttachment) o;
        return Objects.equals(id, otherDataAttachment.id) && Objects.equals(dataAttachment, otherDataAttachment.dataAttachment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, dataAttachment);
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public boolean inline() {
        return false;
    }

    public static Builder builder(String id) {
        return new Builder(id);
    }


    public static class Builder {

        private String id;
        private org.elasticsearch.xpack.watcher.notification.email.DataAttachment dataAttachment;

        private Builder(String id) {
            this.id = id;
        }

        public Builder dataAttachment(org.elasticsearch.xpack.watcher.notification.email.DataAttachment dataAttachment) {
            this.dataAttachment = dataAttachment;
            return this;
        }

        public DataAttachment build() {
            return new DataAttachment(id, dataAttachment);
        }
    }
}
