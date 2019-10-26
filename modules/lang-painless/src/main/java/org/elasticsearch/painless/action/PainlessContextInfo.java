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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.script.ScriptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class PainlessContextInfo implements Writeable, ToXContentObject {

    public static final ParseField NAME = new ParseField("name");
    public static final ParseField CLASSES = new ParseField("classes");
    public static final ParseField IMPORTED_METHODS = new ParseField("imported_methods");
    public static final ParseField CLASS_BINDINGS = new ParseField("class_bindings");
    public static final ParseField INSTANCE_BINDINGS = new ParseField("instance_bindings");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<PainlessContextInfo, Void> PARSER = new ConstructingObjectParser<>(
            PainlessContextInfo.class.getCanonicalName(),
            (v) ->
                    new PainlessContextInfo(
                            (String)v[0],
                            (List<PainlessContextClassInfo>)v[1],
                            (List<PainlessContextMethodInfo>)v[2],
                            (List<PainlessContextClassBindingInfo>)v[3],
                            (List<PainlessContextInstanceBindingInfo>)v[4]
                    )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(),
                (p, c) -> PainlessContextClassInfo.fromXContent(p), CLASSES);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(),
                (p, c) -> PainlessContextMethodInfo.fromXContent(p), IMPORTED_METHODS);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(),
                (p, c) -> PainlessContextClassBindingInfo.fromXContent(p), CLASS_BINDINGS);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(),
                (p, c) -> PainlessContextInstanceBindingInfo.fromXContent(p), INSTANCE_BINDINGS);
    }

    private final String name;
    private final List<PainlessContextClassInfo> classes;
    private final List<PainlessContextMethodInfo> importedMethods;
    private final List<PainlessContextClassBindingInfo> classBindings;
    private final List<PainlessContextInstanceBindingInfo> instanceBindings;

    public PainlessContextInfo(ScriptContext<?> scriptContext, PainlessLookup painlessLookup) {
        this(
                scriptContext.name,
                painlessLookup.getClasses().stream().map(
                        javaClass -> new PainlessContextClassInfo(
                                javaClass,
                                javaClass == painlessLookup.canonicalTypeNameToType(
                                        javaClass.getName().substring(javaClass.getName().lastIndexOf('.') + 1).replace('$', '.')),
                                painlessLookup.lookupPainlessClass(javaClass))
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
        this.name = Objects.requireNonNull(name);
        classes = new ArrayList<>(Objects.requireNonNull(classes));
        classes.sort(Comparator.comparing(PainlessContextClassInfo::getSortValue));
        this.classes = Collections.unmodifiableList(classes);
        importedMethods = new ArrayList<>(Objects.requireNonNull(importedMethods));
        importedMethods.sort(Comparator.comparing(PainlessContextMethodInfo::getSortValue));
        this.importedMethods = Collections.unmodifiableList(importedMethods);
        classBindings = new ArrayList<>(Objects.requireNonNull(classBindings));
        classBindings.sort(Comparator.comparing(PainlessContextClassBindingInfo::getSortValue));
        this.classBindings = Collections.unmodifiableList(classBindings);
        instanceBindings = new ArrayList<>(Objects.requireNonNull(instanceBindings));
        instanceBindings.sort(Comparator.comparing(PainlessContextInstanceBindingInfo::getSortValue));
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

    public static PainlessContextInfo fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME.getPreferredName(), name);
        builder.field(CLASSES.getPreferredName(), classes);
        builder.field(IMPORTED_METHODS.getPreferredName(), importedMethods);
        builder.field(CLASS_BINDINGS.getPreferredName(), classBindings);
        builder.field(INSTANCE_BINDINGS.getPreferredName(), instanceBindings);
        builder.endObject();

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PainlessContextInfo that = (PainlessContextInfo) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(classes, that.classes) &&
                Objects.equals(importedMethods, that.importedMethods) &&
                Objects.equals(classBindings, that.classBindings) &&
                Objects.equals(instanceBindings, that.instanceBindings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, classes, importedMethods, classBindings, instanceBindings);
    }

    @Override
    public String toString() {
        return "PainlessContextInfo{" +
                "name='" + name + '\'' +
                ", classes=" + classes +
                ", importedMethods=" + importedMethods +
                ", classBindings=" + classBindings +
                ", instanceBindings=" + instanceBindings +
                '}';
    }

    public String getName() {
        return name;
    }

    public List<PainlessContextClassInfo> getClasses() {
        return classes;
    }

    public List<PainlessContextMethodInfo> getImportedMethods() {
        return importedMethods;
    }

    public List<PainlessContextClassBindingInfo> getClassBindings() {
        return classBindings;
    }

    public List<PainlessContextInstanceBindingInfo> getInstanceBindings() {
        return instanceBindings;
    }
}
