/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk.serializer;

import io.opentelemetry.sdk.common.InstrumentationScopeInfo;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/** Wire representation of OTel's {@link InstrumentationScopeInfo}.*/
record ScopeRecord(String name, String version, String schemaUrl) implements ToXContentObject {

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField VERSION = new ParseField("version");
    private static final ParseField SCHEMA_URL = new ParseField("schema_url");

    static final ConstructingObjectParser<ScopeRecord, Void> PARSER = new ConstructingObjectParser<>(
        "scope",
        true,
        args -> new ScopeRecord((String) args[0], (String) args[1], (String) args[2])
    );
    static {
        PARSER.declareString(constructorArg(), NAME);
        PARSER.declareString(optionalConstructorArg(), VERSION);
        PARSER.declareString(optionalConstructorArg(), SCHEMA_URL);
    }

    static ScopeRecord fromScope(InstrumentationScopeInfo scope) {
        return new ScopeRecord(scope.getName(), scope.getVersion(), scope.getSchemaUrl());
    }

    InstrumentationScopeInfo toScope() {
        var builder = InstrumentationScopeInfo.builder(name);
        if (version != null) builder.setVersion(version);
        if (schemaUrl != null) builder.setSchemaUrl(schemaUrl);
        return builder.build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field(NAME.getPreferredName(), name);
        if (version != null) b.field(VERSION.getPreferredName(), version);
        if (schemaUrl != null) b.field(SCHEMA_URL.getPreferredName(), schemaUrl);
        b.endObject();
        return b;
    }
}
