/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.slack.message;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DynamicAttachments implements MessageElement {

    private String listPath;
    private Attachment.Template attachment;

    public DynamicAttachments(String listPath, Attachment.Template attachment) {
        this.listPath = listPath;
        this.attachment = attachment;
    }

    @SuppressWarnings("unchecked")
    public List<Attachment> render(TextTemplateEngine engine, Map<String, Object> model, SlackMessageDefaults.AttachmentDefaults defaults) {
        Object value = ObjectPath.eval(listPath, model);
        if ((value instanceof Iterable) == false) {
            throw new IllegalArgumentException("dynamic attachment could not be resolved. expected context [" + listPath + "] to be a " +
                    "list, but found [" + value + "] instead");
        }
        List<Attachment> attachments = new ArrayList<>();
        for (Object obj : (Iterable<Object>) value) {
            if ((obj instanceof Map) == false) {
                throw new IllegalArgumentException("dynamic attachment could not be resolved. expected [" + listPath + "] list to contain" +
                        " key/value pairs, but found [" + obj + "] instead");
            }
            Map<String, Object> attachmentModel = (Map<String, Object>) obj;
            attachments.add(attachment.render(engine, attachmentModel, defaults));
        }
        return attachments;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .field(XField.LIST_PATH.getPreferredName(), listPath)
                .field(XField.TEMPLATE.getPreferredName(), attachment, params)
                .endObject();
    }

    public static DynamicAttachments parse(XContentParser parser) throws IOException {
        String listPath = null;
        Attachment.Template template = null;

        String currentFieldName = null;
        XContentParser.Token token = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (XField.LIST_PATH.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    listPath = parser.text();
                } else {
                    throw new ElasticsearchParseException("could not parse dynamic attachments. expected a string value for [{}] field, " +
                            "but found [{}]", XField.LIST_PATH.getPreferredName(), token);
                }
            } else if (XField.TEMPLATE.match(currentFieldName, parser.getDeprecationHandler())) {
                try {
                    template = Attachment.Template.parse(parser);
                } catch (ElasticsearchParseException pe) {
                    throw new ElasticsearchParseException("could not parse dynamic attachments. failed to parse [{}] field", pe,
                            XField.TEMPLATE.getPreferredName());
                }
            } else {
                throw new ElasticsearchParseException("could not parse dynamic attachments. unexpected field [{}]", currentFieldName);
            }
        }
        if (listPath == null) {
            throw new ElasticsearchParseException("could not parse dynamic attachments. missing required field [{}]",
                    XField.LIST_PATH.getPreferredName());
        }
        if (template == null) {
            throw new ElasticsearchParseException("could not parse dynamic attachments. missing required field [{}]",
                    XField.TEMPLATE.getPreferredName());
        }
        return new DynamicAttachments(listPath, template);
    }

    interface XField extends MessageElement.XField {
        ParseField LIST_PATH = new ParseField("list_path");
        ParseField TEMPLATE = new ParseField("attachment_template");
    }
}
