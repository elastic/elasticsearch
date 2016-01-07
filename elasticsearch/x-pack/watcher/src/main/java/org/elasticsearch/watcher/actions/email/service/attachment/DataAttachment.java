/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service.attachment;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class DataAttachment implements EmailAttachmentParser.EmailAttachment {

    private final String id;
    private final org.elasticsearch.watcher.actions.email.DataAttachment dataAttachment;

    public DataAttachment(String id, org.elasticsearch.watcher.actions.email.DataAttachment dataAttachment) {
        this.id = id;
        this.dataAttachment = dataAttachment;
    }

    public org.elasticsearch.watcher.actions.email.DataAttachment getDataAttachment() {
        return dataAttachment;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(id).startObject(DataAttachmentParser.TYPE);
        if (dataAttachment == org.elasticsearch.watcher.actions.email.DataAttachment.YAML) {
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

    public static Builder builder(String id) {
        return new Builder(id);
    }


    public static class Builder {

        private String id;
        private org.elasticsearch.watcher.actions.email.DataAttachment dataAttachment;

        private Builder(String id) {
            this.id = id;
        }

        public Builder dataAttachment(org.elasticsearch.watcher.actions.email.DataAttachment dataAttachment) {
            this.dataAttachment = dataAttachment;
            return this;
        }

        public DataAttachment build() {
            return new DataAttachment(id, dataAttachment);
        }
    }
}
