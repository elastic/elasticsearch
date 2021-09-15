/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.action;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class PainlessContextInstanceBindingInfo implements Writeable, ToXContentObject {

    public static final ParseField DECLARING = new ParseField("declaring");
    public static final ParseField NAME = new ParseField("name");
    public static final ParseField RTN = new ParseField("return");
    public static final ParseField PARAMETERS = new ParseField("parameters");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<PainlessContextInstanceBindingInfo, Void> PARSER = new ConstructingObjectParser<>(
            PainlessContextInstanceBindingInfo.class.getCanonicalName(),
            (v) ->
                    new PainlessContextInstanceBindingInfo(
                            (String)v[0],
                            (String)v[1],
                            (String)v[2],
                            (List<String>)v[3]
                    )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DECLARING);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), RTN);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), PARAMETERS);
    }

    private final String declaring;
    private final String name;
    private final String rtn;
    private final List<String> parameters;

    public PainlessContextInstanceBindingInfo(PainlessInstanceBinding painlessInstanceBinding) {
        this(
                painlessInstanceBinding.javaMethod.getDeclaringClass().getName(),
                painlessInstanceBinding.javaMethod.getName(),
                painlessInstanceBinding.returnType.getName(),
                painlessInstanceBinding.typeParameters.stream().map(Class::getName).collect(Collectors.toList())

        );
    }

    public PainlessContextInstanceBindingInfo(String declaring, String name, String rtn, List<String> parameters) {
        this.declaring = Objects.requireNonNull(declaring);
        this.name = Objects.requireNonNull(name);
        this.rtn = Objects.requireNonNull(rtn);
        this.parameters = Collections.unmodifiableList(Objects.requireNonNull(parameters));
    }

    public PainlessContextInstanceBindingInfo(StreamInput in) throws IOException {
        declaring = in.readString();
        name = in.readString();
        rtn = in.readString();
        parameters = Collections.unmodifiableList(in.readStringList());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(declaring);
        out.writeString(name);
        out.writeString(rtn);
        out.writeStringCollection(parameters);
    }

    public static PainlessContextInstanceBindingInfo fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(DECLARING.getPreferredName(), declaring);
        builder.field(NAME.getPreferredName(), name);
        builder.field(RTN.getPreferredName(), rtn);
        builder.field(PARAMETERS.getPreferredName(), parameters);
        builder.endObject();

        return builder;
    }

    public String getSortValue() {
        return PainlessLookupUtility.buildPainlessMethodKey(name, parameters.size());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PainlessContextInstanceBindingInfo that = (PainlessContextInstanceBindingInfo) o;
        return Objects.equals(declaring, that.declaring) &&
                Objects.equals(name, that.name) &&
                Objects.equals(rtn, that.rtn) &&
                Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(declaring, name, rtn, parameters);
    }

    @Override
    public String toString() {
        return "PainlessContextInstanceBindingInfo{" +
                "declaring='" + declaring + '\'' +
                ", name='" + name + '\'' +
                ", rtn='" + rtn + '\'' +
                ", parameters=" + parameters +
                '}';
    }

    public String getDeclaring() {
        return declaring;
    }

    public String getName() {
        return name;
    }

    public String getRtn() {
        return rtn;
    }

    public List<String> getParameters() {
        return parameters;
    }
}
