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
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;

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
            (v) ->
                    new PainlessContextConstructorInfo(
                            (String)v[0],
                            (List<String>)v[1]
                    )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DECLARING);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), PARAMETERS);
    }

    public PainlessContextConstructorInfo(PainlessConstructor painlessConstructor) {
        this (
                painlessConstructor.javaConstructor.getDeclaringClass().getName(),
                painlessConstructor.typeParameters.stream().map(Class::getName).collect(Collectors.toList())
        );
    }

    public PainlessContextConstructorInfo(String declaring, List<String> parameters) {
        this.declaring = Objects.requireNonNull(declaring);
        this.parameters = Collections.unmodifiableList(Objects.requireNonNull(parameters));
    }

    public PainlessContextConstructorInfo(StreamInput in) throws IOException {
        declaring = in.readString();
        parameters = Collections.unmodifiableList(in.readStringList());
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
        return Objects.equals(declaring, that.declaring) &&
                Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(declaring, parameters);
    }

    @Override
    public String toString() {
        return "PainlessContextConstructorInfo{" +
                "declaring='" + declaring + '\'' +
                ", parameters=" + parameters +
                '}';
    }

    public String getDeclaring() {
        return declaring;
    }

    public List<String> getParameters() {
        return parameters;
    }
}
