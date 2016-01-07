/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service.attachment;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class EmailAttachments implements ToXContent {

    public interface Fields {
        ParseField ATTACHMENTS = new ParseField("attachments");
    }

    private final List<EmailAttachmentParser.EmailAttachment> attachments;

    public EmailAttachments(List<EmailAttachmentParser.EmailAttachment> attachments) {
        this.attachments = attachments;
    }

    public List<EmailAttachmentParser.EmailAttachment> getAttachments() {
        return attachments;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (attachments != null && attachments.size() > 0) {
            builder.startObject(Fields.ATTACHMENTS.getPreferredName());
            for (EmailAttachmentParser.EmailAttachment attachment : attachments) {
                attachment.toXContent(builder, params);
            }
            builder.endObject();
        }

        return builder;
    }
}
