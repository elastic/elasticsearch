/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email.attachment;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;

public class DataAttachmentParserTests extends ESTestCase {

    public void testSerializationWorks() throws Exception {
        Map<String, EmailAttachmentParser<?>> attachmentParsers = new HashMap<>();
        attachmentParsers.put(DataAttachmentParser.TYPE, new DataAttachmentParser());
        EmailAttachmentsParser emailAttachmentsParser = new EmailAttachmentsParser(attachmentParsers);

        String id = "some-id";
        XContentBuilder builder = jsonBuilder().startObject().startObject(id)
                .startObject(DataAttachmentParser.TYPE).field("format", randomFrom("yaml", "json")).endObject()
                .endObject().endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        logger.info("JSON: {}", Strings.toString(builder));

        EmailAttachments emailAttachments = emailAttachmentsParser.parse(parser);
        assertThat(emailAttachments.getAttachments(), hasSize(1));

        XContentBuilder toXcontentBuilder = jsonBuilder().startObject();
        List<EmailAttachmentParser.EmailAttachment> attachments = new ArrayList<>(emailAttachments.getAttachments());
        attachments.get(0).toXContent(toXcontentBuilder, ToXContent.EMPTY_PARAMS);
        toXcontentBuilder.endObject();
        assertThat(Strings.toString(toXcontentBuilder), is(Strings.toString(builder)));
    }

}
