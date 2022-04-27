/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class PainlessContextFieldInfo implements Writeable, ToXContentObject {

    public static final ParseField DECLARING = new ParseField("declaring");
    public static final ParseField NAME = new ParseField("name");
    public static final ParseField TYPE = new ParseField("type");

    private static final ConstructingObjectParser<PainlessContextFieldInfo, Void> PARSER = new ConstructingObjectParser<>(
        PainlessContextFieldInfo.class.getCanonicalName(),
        (v) -> new PainlessContextFieldInfo((String) v[0], (String) v[1], (String) v[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DECLARING);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE);
    }

    private final String declaring;
    private final String name;
    private final String type;

    public PainlessContextFieldInfo(PainlessField painlessField) {
        this(
            painlessField.javaField().getDeclaringClass().getName(),
            painlessField.javaField().getName(),
            PainlessContextTypeInfo.getType(painlessField.typeParameter().getName())
        );
    }

    public PainlessContextFieldInfo(String declaring, String name, String type) {
        this.declaring = Objects.requireNonNull(declaring);
        this.name = Objects.requireNonNull(name);
        this.type = Objects.requireNonNull(type);
    }

    public PainlessContextFieldInfo(StreamInput in) throws IOException {
        declaring = in.readString();
        name = in.readString();
        type = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(declaring);
        out.writeString(name);
        out.writeString(type);
    }

    public static PainlessContextFieldInfo fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DECLARING.getPreferredName(), declaring);
        builder.field(NAME.getPreferredName(), name);
        builder.field(TYPE.getPreferredName(), type);
        builder.endObject();

        return builder;
    }

    public String getSortValue() {
        return PainlessLookupUtility.buildPainlessFieldKey(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PainlessContextFieldInfo that = (PainlessContextFieldInfo) o;
        return Objects.equals(declaring, that.declaring) && Objects.equals(name, that.name) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(declaring, name, type);
    }

    @Override
    public String toString() {
        return "PainlessContextFieldInfo{" + "declaring='" + declaring + '\'' + ", name='" + name + '\'' + ", type='" + type + '\'' + '}';
    }

    public String getDeclaring() {
        return declaring;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }
}
