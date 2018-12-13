/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.email.attachment;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EmailAttachmentsParser {
    private final Map<String, EmailAttachmentParser> parsers;

    public EmailAttachmentsParser(Map<String, EmailAttachmentParser> parsers) {
        this.parsers = Collections.unmodifiableMap(parsers);
    }

    public EmailAttachments parse(XContentParser parser) throws IOException {
        List<EmailAttachmentParser.EmailAttachment> attachments = new ArrayList<>();
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else {
                if (token == XContentParser.Token.START_OBJECT && currentFieldName != null) {
                    String currentAttachmentType = null;
                    if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                        currentAttachmentType = parser.currentName();
                    }
                    parser.nextToken();

                    EmailAttachmentParser emailAttachmentParser = parsers.get(currentAttachmentType);
                    if (emailAttachmentParser == null) {
                        throw new ElasticsearchParseException("Cannot parse attachment of type [{}]", currentAttachmentType);
                    }
                    EmailAttachmentParser.EmailAttachment emailAttachment = emailAttachmentParser.parse(currentFieldName, parser);
                    attachments.add(emailAttachment);
                    // one further to skip the end_object from the attachment
                    parser.nextToken();
                }
            }
        }

        return new EmailAttachments(attachments);
    }

    public Map<String, EmailAttachmentParser> getParsers() {
        return parsers;
    }
}
