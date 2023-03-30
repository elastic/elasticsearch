/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search;

import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TemplateParamValidator implements ToXContentObject, Writeable {

    public static final ParseField VALIDATOR_SOURCE = new ParseField("source");
    private static final SpecVersion.VersionFlag SCHEMA_VERSION = SpecVersion.VersionFlag.V7;
    private static final ConstructingObjectParser<TemplateParamValidator, Void> PARSER = new ConstructingObjectParser<>(
        "param_validation",
        p -> new TemplateParamValidator((String) p[0])
    );

    static {
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            return Strings.toString(builder.copyCurrentStructure(p));
        }, VALIDATOR_SOURCE);
    }

    private final JsonSchema jsonSchema;

    public TemplateParamValidator(StreamInput in) throws IOException {
        this(in.readString());
    }

    public TemplateParamValidator(String validationSource) {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SCHEMA_VERSION);
        this.jsonSchema = factory.getSchema(validationSource);
    }

    public static TemplateParamValidator parse(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    public void validate(Map<String, Object> params) {

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (XContentType.JSON.equals(builder.contentType())) {
            try (InputStream stream = new BytesArray(getSchemaAsString()).streamInput()) {
                builder.rawField(VALIDATOR_SOURCE.getPreferredName(), stream);
            }
        } else {
            builder.field(VALIDATOR_SOURCE.getPreferredName(), getSchemaAsString());
        }
        builder.endObject();

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(getSchemaAsString());
    }

    private String getSchemaAsString() {
        return jsonSchema.getSchemaNode().toString();
    }
}
