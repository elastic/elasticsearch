/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email.attachment;

import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.notification.email.Attachment;

import java.io.IOException;

/**
 * Marker interface for email attachments that have an additional execution step and are used by
 * EmailAttachmentParser class
 */
public interface EmailAttachmentParser<T extends EmailAttachmentParser.EmailAttachment> {

    interface EmailAttachment extends ToXContentFragment {
        /**
         * @return A type to identify the email attachment, same as the parser identifier
         */
        String type();

        /**
         * @return The id of this attachment
         */
        String id();

        /**
         * Allows the attachment to decide of it should be of disposition type attachment or inline, which is important
         * for being able to display inside of desktop email clients
         *
         * @return a boolean flagging this attachment as being inline
         */
        boolean inline();
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
    Attachment toAttachment(WatchExecutionContext context, Payload payload, T attachment) throws IOException;

}
