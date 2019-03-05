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
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.script.ScriptContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PainlessContextInfo implements Writeable, ToXContentObject {
    public final ParseField NAME = new ParseField("name");
    public final ParseField CLAZZ = new ParseField("class");
    public final ParseField IMPORTED_METHOD = new ParseField("imported_method");
    public final ParseField ClASS_BINDING = new ParseField("class_binding");
    public final ParseField INSTANCE_BINDING = new ParseField("instance_binding");

    private final String name;
    private final List<PainlessContextClassInfo> classes;
    private final List<PainlessContextMethodInfo> importedMethods;
    private final List<PainlessContextClassBindingInfo> classBindings;
    private final List<PainlessContextInstanceBindingInfo> instanceBindings;

    public PainlessContextInfo(ScriptContext<?> scriptContext, PainlessLookup painlessLookup) {
        this(
                scriptContext.name,
                painlessLookup.getClasses().stream().map(
                        javaClass -> new PainlessContextClassInfo(javaClass, painlessLookup.lookupPainlessClass(javaClass))
                ).collect(Collectors.toList()),
                painlessLookup.getImportedPainlessMethodsKeys().stream().map(importedPainlessMethodKey -> {
                    String[] split = importedPainlessMethodKey.split("/");
                    String importedPainlessMethodName = split[0];
                    int importedPainlessMethodArity = Integer.parseInt(split[1]);
                    PainlessMethod importedPainlessMethod =
                            painlessLookup.lookupImportedPainlessMethod(importedPainlessMethodName, importedPainlessMethodArity);
                    return new PainlessContextMethodInfo(importedPainlessMethod);
                }).collect(Collectors.toList()),
                painlessLookup.getPainlessClassBindingsKeys().stream().map(painlessClassBindingKey -> {
                    String[] split = painlessClassBindingKey.split("/");
                    String painlessClassBindingName = split[0];
                    int painlessClassBindingArity = Integer.parseInt(split[1]);
                    PainlessClassBinding painlessClassBinding =
                            painlessLookup.lookupPainlessClassBinding(painlessClassBindingName, painlessClassBindingArity);
                    return new PainlessContextClassBindingInfo(painlessClassBinding);
                }).collect(Collectors.toList()),
                painlessLookup.getPainlessInstanceBindingsKeys().stream().map(painlessInstanceBindingKey -> {
                    String[] split = painlessInstanceBindingKey.split("/");
                    String painlessInstanceBindingName = split[0];
                    int painlessInstanceBindingArity = Integer.parseInt(split[1]);
                    PainlessInstanceBinding painlessInstanceBinding =
                            painlessLookup.lookupPainlessInstanceBinding(painlessInstanceBindingName, painlessInstanceBindingArity);
                    return new PainlessContextInstanceBindingInfo(painlessInstanceBinding);
                }).collect(Collectors.toList())
        );
    }

    public PainlessContextInfo(String name, List<PainlessContextClassInfo> classes, List<PainlessContextMethodInfo> importedMethods,
            List<PainlessContextClassBindingInfo> classBindings, List<PainlessContextInstanceBindingInfo> instanceBindings) {
        this.name = name;
        this.classes = Collections.unmodifiableList(classes);
        this.importedMethods = Collections.unmodifiableList(importedMethods);
        this.classBindings = Collections.unmodifiableList(classBindings);
        this.instanceBindings = Collections.unmodifiableList(instanceBindings);
    }

    public PainlessContextInfo(StreamInput in) throws IOException {
        name = in.readString();
        classes = Collections.unmodifiableList(in.readList(PainlessContextClassInfo::new));
        importedMethods = Collections.unmodifiableList(in.readList(PainlessContextMethodInfo::new));
        classBindings = Collections.unmodifiableList(in.readList(PainlessContextClassBindingInfo::new));
        instanceBindings = Collections.unmodifiableList(in.readList(PainlessContextInstanceBindingInfo::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeList(classes);
        out.writeList(importedMethods);
        out.writeList(classBindings);
        out.writeList(instanceBindings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME.getPreferredName(), name);

        for (PainlessContextClassInfo clazz : classes) {
            builder.field(CLAZZ.getPreferredName(), clazz);
        }

        for (PainlessContextMethodInfo importedMethod : importedMethods) {
            builder.field(IMPORTED_METHOD.getPreferredName(), importedMethod);
        }

        for (PainlessContextClassBindingInfo classBinding : classBindings) {
            builder.field(ClASS_BINDING.getPreferredName(), classBinding);
        }

        for (PainlessContextInstanceBindingInfo instanceBinding : instanceBindings) {
            builder.field(INSTANCE_BINDING.getPreferredName(), instanceBinding);
        }

        builder.endObject();

        return builder;
    }
}
