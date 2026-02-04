/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public record CCMModel(SecureString apiKey) implements Writeable, ToXContentObject {

    private static final String API_KEY_FIELD = "api_key";
    private static final ConstructingObjectParser<CCMModel, Void> PARSER = new ConstructingObjectParser<>(
        CCMModel.class.getSimpleName(),
        true,
        args -> new CCMModel(new SecureString(((String) args[0]).toCharArray()))
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField(API_KEY_FIELD));
    }

    public static CCMModel parse(org.elasticsearch.xcontent.XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public CCMModel {
        Objects.requireNonNull(apiKey);
    }

    public CCMModel(StreamInput in) throws IOException {
        this(in.readSecureString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeSecureString(apiKey);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(API_KEY_FIELD, apiKey.toString());
        builder.endObject();
        return builder;
    }

    public static CCMModel fromXContentBytes(BytesReference bytes) throws IOException {
        try (var parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, bytes, XContentType.JSON)) {
            return parse(parser);
        }
    }
}
