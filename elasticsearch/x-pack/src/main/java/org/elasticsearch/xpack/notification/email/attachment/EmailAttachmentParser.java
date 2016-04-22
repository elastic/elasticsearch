/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.email.attachment;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.xpack.notification.email.Attachment;

import java.io.IOException;

/**
 * Marker interface for email attachments that have an additional execution step and are used by
 * EmailAttachmentParser class
 */
public interface EmailAttachmentParser<T extends EmailAttachmentParser.EmailAttachment> {

    interface EmailAttachment extends ToXContent {
        /**
         * @return A type to identify the email attachment, same as the parser identifier
         */
        String type();

        /**
         * @return The id of this attachment
         */
        String id();
    }

    /**
     * @return An identifier of this parser
     */
    String type();

    /**
     * A parser to create an EmailAttachment, that is serializable and does not execute anything
     *
     * @param id The id of this attachment, parsed from the outer content
     * @param parser The XContentParser used for parsing
     * @return A concrete EmailAttachment
     * @throws IOException in case parsing fails
     */
    T parse(String id, XContentParser parser) throws IOException;

    /**
     * Converts an email attachment to an attachment, potentially executing code like an HTTP request
     * @param context The WatchExecutionContext supplied with the whole watch execution
     * @param payload The Payload supplied with the action
     * @param attachment The typed attachment
     * @return An attachment that is ready to be used in a MimeMessage
     */
    Attachment toAttachment(WatchExecutionContext context, Payload payload, T attachment) throws ElasticsearchException;

}
