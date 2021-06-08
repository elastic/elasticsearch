/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.watch.Payload;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.core.watcher.support.Exceptions.illegalArgument;

public enum DataAttachment implements ToXContentObject {

    YAML() {
        @Override
        public String contentType() {
            return XContentType.YAML.mediaType();
        }

        @Override
        public Attachment create(String id, Map<String, Object> data) {
            return new Attachment.XContent.Yaml(id, id, new Payload.Simple(data));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field(Field.FORMAT.getPreferredName(), "yaml").endObject();
        }
    },

    JSON() {
        @Override
        public String contentType() {
            return XContentType.JSON.mediaType();
        }

        @Override
        public Attachment create(String id, Map<String, Object> data) {
            return new Attachment.XContent.Json(id, id, new Payload.Simple(data));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field(Field.FORMAT.getPreferredName(), "json").endObject();
        }
    };

    public static DataAttachment DEFAULT = YAML;

    public abstract String contentType();

    public abstract Attachment create(String id, Map<String, Object> data);

    public static DataAttachment resolve(String format) {
        switch (format.toLowerCase(Locale.ROOT)) {
            case "yaml": return YAML;
            case "json": return JSON;
            default:
                throw illegalArgument("unknown data attachment format [{}]", format);
        }
    }

    public static DataAttachment parse(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        if (token == XContentParser.Token.VALUE_BOOLEAN) {
            return parser.booleanValue() ? DEFAULT : null;
        }
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("could not parse data attachment. expected either a boolean value or an object but " +
                    "found [{}] instead", token);
        }

        DataAttachment dataAttachment = DEFAULT;

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (currentFieldName == null) {
                throw new ElasticsearchParseException("could not parse data attachment. expected [{}] field but found [{}] instead",
                        Field.FORMAT.getPreferredName(), token);
            } else if (Field.FORMAT.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    dataAttachment = resolve(parser.text());
                } else {
                    throw new ElasticsearchParseException("could not parse data attachment. expected string value for [{}] field but " +
                            "found [{}] instead", currentFieldName, token);
                }
            } else {
                throw new ElasticsearchParseException("could not parse data attachment. unexpected field [{}]", currentFieldName);
            }
        }

        return dataAttachment;
    }

    interface Field {
        ParseField FORMAT = new ParseField("format");
    }
}
