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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.painless.lookup.PainlessClass;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PainlessContextClassInfo implements Writeable, ToXContentObject {

    public static final ParseField NAME = new ParseField("name");
    public static final ParseField CONSTRUCTOR = new ParseField("constructor");
    public static final ParseField STATIC_METHOD = new ParseField("static_method");
    public static final ParseField METHOD = new ParseField("method");
    public static final ParseField STATIC_FIELD = new ParseField("static_field");
    public static final ParseField FIELD = new ParseField("field");

    private final String name;
    private final List<PainlessContextConstructorInfo> constructors;
    private final List<PainlessContextMethodInfo> staticMethods;
    private final List<PainlessContextMethodInfo> methods;
    private final List<PainlessContextFieldInfo> staticFields;
    private final List<PainlessContextFieldInfo> fields;

    public PainlessContextClassInfo(Class<?> javaClass, PainlessClass painlessClass) {
        this(
                javaClass.getName(),
                painlessClass.constructors.values().stream().map(PainlessContextConstructorInfo::new).collect(Collectors.toList()),
                painlessClass.staticMethods.values().stream().map(PainlessContextMethodInfo::new).collect(Collectors.toList()),
                painlessClass.methods.values().stream().map(PainlessContextMethodInfo::new).collect(Collectors.toList()),
                painlessClass.staticFields.values().stream().map(PainlessContextFieldInfo::new).collect(Collectors.toList()),
                painlessClass.fields.values().stream().map(PainlessContextFieldInfo::new).collect(Collectors.toList())
        );
    }

    public PainlessContextClassInfo(String name, List<PainlessContextConstructorInfo> constructors,
            List<PainlessContextMethodInfo> staticMethods, List<PainlessContextMethodInfo> methods,
            List<PainlessContextFieldInfo> staticFields, List<PainlessContextFieldInfo> fields) {

        this.name = name;
        this.constructors = Collections.unmodifiableList(constructors);
        this.staticMethods = Collections.unmodifiableList(staticMethods);
        this.methods = Collections.unmodifiableList(methods);
        this.staticFields = Collections.unmodifiableList(staticFields);
        this.fields = Collections.unmodifiableList(fields);
    }

    public PainlessContextClassInfo(StreamInput in) throws IOException {
        name = in.readString();
        constructors = Collections.unmodifiableList(in.readList(PainlessContextConstructorInfo::new));
        staticMethods = Collections.unmodifiableList(in.readList(PainlessContextMethodInfo::new));
        methods = Collections.unmodifiableList(in.readList(PainlessContextMethodInfo::new));
        staticFields = Collections.unmodifiableList(in.readList(PainlessContextFieldInfo::new));
        fields = Collections.unmodifiableList(in.readList(PainlessContextFieldInfo::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeList(constructors);
        out.writeList(staticMethods);
        out.writeList(methods);
        out.writeList(staticFields);
        out.writeList(fields);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME.getPreferredName(), name);

        for (PainlessContextConstructorInfo constructor : constructors) {
            builder.field(CONSTRUCTOR.getPreferredName(), constructor);
        }

        for (PainlessContextMethodInfo staticMethod : staticMethods) {
            builder.field(STATIC_METHOD.getPreferredName(), staticMethod);
        }

        for (PainlessContextMethodInfo method : methods) {
            builder.field(METHOD.getPreferredName(), method);
        }

        for (PainlessContextFieldInfo staticField : staticFields) {
            builder.field(STATIC_FIELD.getPreferredName(), staticField);
        }

        for (PainlessContextFieldInfo field : fields) {
            builder.field(FIELD.getPreferredName(), field);
        }

        builder.endObject();

        return builder;
    }
}
