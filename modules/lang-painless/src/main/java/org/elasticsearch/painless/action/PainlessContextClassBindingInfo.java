/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless.action;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class PainlessContextClassBindingInfo implements Writeable, ToXContentObject {

    public static final ParseField DECLARING = new ParseField("declaring");
    public static final ParseField NAME = new ParseField("name");
    public static final ParseField RTN = new ParseField("return");
    public static final ParseField READ_ONLY = new ParseField("read_only");
    public static final ParseField PARAMETERS = new ParseField("parameters");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<PainlessContextClassBindingInfo, Void> PARSER = new ConstructingObjectParser<>(
            PainlessContextClassBindingInfo.class.getCanonicalName(),
            (v) ->
                    new PainlessContextClassBindingInfo(
                            (String)v[0],
                            (String)v[1],
                            (String)v[2],
                            (int)v[3],
                            (List<String>)v[4]
                    )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DECLARING);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), RTN);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), READ_ONLY);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), PARAMETERS);
    }

    private final String declaring;
    private final String name;
    private final String rtn;
    private final int readOnly;
    private final List<String> parameters;

    public PainlessContextClassBindingInfo(PainlessClassBinding painlessClassBinding) {
        this(
                painlessClassBinding.javaMethod.getDeclaringClass().getName(),
                painlessClassBinding.javaMethod.getName(),
                painlessClassBinding.returnType.getName(),
                painlessClassBinding.javaConstructor.getParameterCount(),
                painlessClassBinding.typeParameters.stream().map(Class::getName).collect(Collectors.toList())
        );
    }

    public PainlessContextClassBindingInfo(String declaring, String name, String rtn, int readOnly, List<String> parameters) {
        this.declaring = Objects.requireNonNull(declaring);
        this.name = Objects.requireNonNull(name);
        this.rtn = Objects.requireNonNull(rtn);
        this.readOnly = readOnly;
        this.parameters = Collections.unmodifiableList(Objects.requireNonNull(parameters));
    }

    public PainlessContextClassBindingInfo(StreamInput in) throws IOException {
        declaring = in.readString();
        name = in.readString();
        rtn = in.readString();
        readOnly = in.readInt();
        parameters = Collections.unmodifiableList(in.readStringList());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(declaring);
        out.writeString(name);
        out.writeString(rtn);
        out.writeInt(readOnly);
        out.writeStringCollection(parameters);
    }

    public static PainlessContextClassBindingInfo fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(DECLARING.getPreferredName(), declaring);
        builder.field(NAME.getPreferredName(), name);
        builder.field(RTN.getPreferredName(), rtn);
        builder.field(READ_ONLY.getPreferredName(), readOnly);
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
        PainlessContextClassBindingInfo that = (PainlessContextClassBindingInfo) o;
        return readOnly == that.readOnly &&
                Objects.equals(declaring, that.declaring) &&
                Objects.equals(name, that.name) &&
                Objects.equals(rtn, that.rtn) &&
                Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(declaring, name, rtn, readOnly, parameters);
    }

    @Override
    public String toString() {
        return "PainlessContextClassBindingInfo{" +
                "declaring='" + declaring + '\'' +
                ", name='" + name + '\'' +
                ", rtn='" + rtn + '\'' +
                ", readOnly=" + readOnly +
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

    public int getReadOnly() {
        return readOnly;
    }

    public List<String> getParameters() {
        return parameters;
    }
}
