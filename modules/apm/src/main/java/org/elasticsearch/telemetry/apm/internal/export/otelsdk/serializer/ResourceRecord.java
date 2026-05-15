/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk.serializer;

import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/** Wire representation of OTel's {@link Resource}. */
record ResourceRecord(List<AttributeRecord> attributes, String schemaUrl) implements ToXContentObject {

    private static final ParseField ATTRIBUTES = new ParseField("attributes");
    private static final ParseField SCHEMA_URL = new ParseField("schema_url");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<ResourceRecord, Void> PARSER = new ConstructingObjectParser<>(
        "resource",
        true,
        args -> new ResourceRecord((List<AttributeRecord>) args[0], (String) args[1])
    );
    static {
        PARSER.declareObjectArray(constructorArg(), AttributeRecord.PARSER, ATTRIBUTES);
        PARSER.declareString(optionalConstructorArg(), SCHEMA_URL);
    }

    static ResourceRecord fromResource(Resource r) {
        return new ResourceRecord(AttributeRecord.fromAttributes(r.getAttributes()), r.getSchemaUrl());
    }

    Resource toResource() {
        return Resource.create(AttributeRecord.toAttributes(attributes), schemaUrl);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        AttributeRecord.writeArray(b, ATTRIBUTES.getPreferredName(), attributes, params);
        if (schemaUrl != null) b.field(SCHEMA_URL.getPreferredName(), schemaUrl);
        b.endObject();
        return b;
    }
}
