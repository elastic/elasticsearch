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
import org.elasticsearch.painless.lookup.PainlessClass;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class PainlessContextClassInfo implements Writeable, ToXContentObject {

    public static final ParseField NAME = new ParseField("name");
    public static final ParseField IMPORTED = new ParseField("imported");
    public static final ParseField CONSTRUCTORS = new ParseField("constructors");
    public static final ParseField STATIC_METHODS = new ParseField("static_methods");
    public static final ParseField METHODS = new ParseField("methods");
    public static final ParseField STATIC_FIELDS = new ParseField("static_fields");
    public static final ParseField FIELDS = new ParseField("fields");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<PainlessContextClassInfo, Void> PARSER = new ConstructingObjectParser<>(
        PainlessContextClassInfo.class.getCanonicalName(),
        (v) -> new PainlessContextClassInfo(
            (String) v[0],
            (boolean) v[1],
            (List<PainlessContextConstructorInfo>) v[2],
            (List<PainlessContextMethodInfo>) v[3],
            (List<PainlessContextMethodInfo>) v[4],
            (List<PainlessContextFieldInfo>) v[5],
            (List<PainlessContextFieldInfo>) v[6]
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), IMPORTED);
        PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> PainlessContextConstructorInfo.fromXContent(p),
            CONSTRUCTORS
        );
        PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> PainlessContextMethodInfo.fromXContent(p),
            STATIC_METHODS
        );
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> PainlessContextMethodInfo.fromXContent(p), METHODS);
        PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> PainlessContextFieldInfo.fromXContent(p),
            STATIC_FIELDS
        );
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> PainlessContextFieldInfo.fromXContent(p), FIELDS);
    }

    private final String name;
    private final boolean imported;
    private final List<PainlessContextConstructorInfo> constructors;
    private final List<PainlessContextMethodInfo> staticMethods;
    private final List<PainlessContextMethodInfo> methods;
    private final List<PainlessContextFieldInfo> staticFields;
    private final List<PainlessContextFieldInfo> fields;

    public PainlessContextClassInfo(Class<?> javaClass, boolean imported, PainlessClass painlessClass) {
        this(
            javaClass.getName(),
            imported,
            painlessClass.constructors.values().stream().map(PainlessContextConstructorInfo::new).collect(Collectors.toList()),
            painlessClass.staticMethods.values().stream().map(PainlessContextMethodInfo::new).collect(Collectors.toList()),
            painlessClass.methods.values().stream().map(PainlessContextMethodInfo::new).collect(Collectors.toList()),
            painlessClass.staticFields.values().stream().map(PainlessContextFieldInfo::new).collect(Collectors.toList()),
            painlessClass.fields.values().stream().map(PainlessContextFieldInfo::new).collect(Collectors.toList())
        );
    }

    public PainlessContextClassInfo(
        String name,
        boolean imported,
        List<PainlessContextConstructorInfo> constructors,
        List<PainlessContextMethodInfo> staticMethods,
        List<PainlessContextMethodInfo> methods,
        List<PainlessContextFieldInfo> staticFields,
        List<PainlessContextFieldInfo> fields
    ) {

        this.name = Objects.requireNonNull(name);
        this.imported = imported;
        constructors = new ArrayList<>(Objects.requireNonNull(constructors));
        constructors.sort(Comparator.comparing(PainlessContextConstructorInfo::getSortValue));
        this.constructors = Collections.unmodifiableList(constructors);
        staticMethods = new ArrayList<>(Objects.requireNonNull(staticMethods));
        staticMethods.sort(Comparator.comparing(PainlessContextMethodInfo::getSortValue));
        this.staticMethods = Collections.unmodifiableList(staticMethods);
        methods = new ArrayList<>(Objects.requireNonNull(methods));
        methods.sort(Comparator.comparing(PainlessContextMethodInfo::getSortValue));
        this.methods = Collections.unmodifiableList(methods);
        staticFields = new ArrayList<>(Objects.requireNonNull(staticFields));
        staticFields.sort(Comparator.comparing(PainlessContextFieldInfo::getSortValue));
        this.staticFields = Collections.unmodifiableList(staticFields);
        fields = new ArrayList<>(Objects.requireNonNull(fields));
        fields.sort(Comparator.comparing(PainlessContextFieldInfo::getSortValue));
        this.fields = Collections.unmodifiableList(fields);
    }

    public PainlessContextClassInfo(StreamInput in) throws IOException {
        name = in.readString();
        imported = in.readBoolean();
        constructors = in.readImmutableList(PainlessContextConstructorInfo::new);
        staticMethods = in.readImmutableList(PainlessContextMethodInfo::new);
        methods = in.readImmutableList(PainlessContextMethodInfo::new);
        staticFields = in.readImmutableList(PainlessContextFieldInfo::new);
        fields = in.readImmutableList(PainlessContextFieldInfo::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeBoolean(imported);
        out.writeList(constructors);
        out.writeList(staticMethods);
        out.writeList(methods);
        out.writeList(staticFields);
        out.writeList(fields);
    }

    public static PainlessContextClassInfo fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME.getPreferredName(), name);
        builder.field(IMPORTED.getPreferredName(), imported);
        builder.field(CONSTRUCTORS.getPreferredName(), constructors);
        builder.field(STATIC_METHODS.getPreferredName(), staticMethods);
        builder.field(METHODS.getPreferredName(), methods);
        builder.field(STATIC_FIELDS.getPreferredName(), staticFields);
        builder.field(FIELDS.getPreferredName(), fields);
        builder.endObject();

        return builder;
    }

    public String getSortValue() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PainlessContextClassInfo that = (PainlessContextClassInfo) o;
        return imported == that.imported
            && Objects.equals(name, that.name)
            && Objects.equals(constructors, that.constructors)
            && Objects.equals(staticMethods, that.staticMethods)
            && Objects.equals(methods, that.methods)
            && Objects.equals(staticFields, that.staticFields)
            && Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, imported, constructors, staticMethods, methods, staticFields, fields);
    }

    @Override
    public String toString() {
        return "PainlessContextClassInfo{"
            + "name='"
            + name
            + '\''
            + ", imported="
            + imported
            + ", constructors="
            + constructors
            + ", staticMethods="
            + staticMethods
            + ", methods="
            + methods
            + ", staticFields="
            + staticFields
            + ", fields="
            + fields
            + '}';
    }

    public String getName() {
        return name;
    }

    public boolean isImported() {
        return imported;
    }

    public List<PainlessContextConstructorInfo> getConstructors() {
        return constructors;
    }

    public List<PainlessContextMethodInfo> getStaticMethods() {
        return staticMethods;
    }

    public List<PainlessContextMethodInfo> getMethods() {
        return methods;
    }

    public List<PainlessContextFieldInfo> getStaticFields() {
        return staticFields;
    }

    public List<PainlessContextFieldInfo> getFields() {
        return fields;
    }
}
