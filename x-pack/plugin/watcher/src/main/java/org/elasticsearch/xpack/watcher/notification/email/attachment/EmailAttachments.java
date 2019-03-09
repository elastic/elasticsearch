/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.email.attachment;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

public class EmailAttachments implements ToXContentFragment {

    public static final EmailAttachments EMPTY_ATTACHMENTS = new EmailAttachments(
            Collections.<EmailAttachmentParser.EmailAttachment>emptyList());

    public interface Fields {
        ParseField ATTACHMENTS = new ParseField("attachments");
    }

    private final Collection<EmailAttachmentParser.EmailAttachment> attachments;

    public EmailAttachments(Collection<EmailAttachmentParser.EmailAttachment> attachments) {
        this.attachments = attachments;
    }

    public Collection<EmailAttachmentParser.EmailAttachment> getAttachments() {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EmailAttachments other = (EmailAttachments) o;
        return Objects.equals(attachments, other.attachments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attachments);
    }
}
