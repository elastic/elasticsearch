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
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class PainlessContextConstructorInfo implements Writeable, ToXContentObject {

    public static final ParseField DECLARING = new ParseField("declaring");
    public static final ParseField PARAMETERS = new ParseField("parameters");

    private final String declaring;
    private final List<String> parameters;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<PainlessContextConstructorInfo, Void> PARSER = new ConstructingObjectParser<>(
        PainlessContextConstructorInfo.class.getCanonicalName(),
        (v) -> new PainlessContextConstructorInfo((String) v[0], (List<String>) v[1])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DECLARING);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), PARAMETERS);
    }

    public PainlessContextConstructorInfo(PainlessConstructor painlessConstructor) {
        this(
            painlessConstructor.javaConstructor().getDeclaringClass().getName(),
            painlessConstructor.typeParameters()
                .stream()
                .map(c -> PainlessContextTypeInfo.getType(c.getName()))
                .collect(Collectors.toList())
        );
    }

    public PainlessContextConstructorInfo(String declaring, List<String> parameters) {
        this.declaring = Objects.requireNonNull(declaring);
        this.parameters = Collections.unmodifiableList(Objects.requireNonNull(parameters));
    }

    public PainlessContextConstructorInfo(StreamInput in) throws IOException {
        declaring = in.readString();
        parameters = in.readImmutableList(StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(declaring);
        out.writeStringCollection(parameters);
    }

    public static PainlessContextConstructorInfo fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(DECLARING.getPreferredName(), declaring);
        builder.field(PARAMETERS.getPreferredName(), parameters);
        builder.endObject();

        return builder;
    }

    public String getSortValue() {
        return PainlessLookupUtility.buildPainlessConstructorKey(parameters.size());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PainlessContextConstructorInfo that = (PainlessContextConstructorInfo) o;
        return Objects.equals(declaring, that.declaring) && Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(declaring, parameters);
    }

    @Override
    public String toString() {
        return "PainlessContextConstructorInfo{" + "declaring='" + declaring + '\'' + ", parameters=" + parameters + '}';
    }

    public String getDeclaring() {
        return declaring;
    }

    public List<String> getParameters() {
        return parameters;
    }
}
