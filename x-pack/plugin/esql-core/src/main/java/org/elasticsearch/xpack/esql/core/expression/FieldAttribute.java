/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;
import org.elasticsearch.xpack.esql.core.util.PlanStreamOutput;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.util.PlanStreamInput.readCachedStringWithVersionCheck;
import static org.elasticsearch.xpack.esql.core.util.PlanStreamOutput.writeCachedStringWithVersionCheck;

/**
 * Attribute for an ES field.
 * This class offers:
 * - name - the name of the attribute, but not necessarily of the field.
 * - The raw EsField representing the field; for parent.child.grandchild this is just grandchild.
 * - parentName - the full path to the immediate parent of the field, e.g. parent.child (without .grandchild)
 *
 * To adequately represent e.g. union types, the name of the attribute can be altered because we may have multiple synthetic field
 * attributes that really belong to the same underlying field. For instance, if a multi-typed field is used both as {@code field::string}
 * and {@code field::ip}, we'll generate 2 field attributes called {@code $$field$converted_to$string} and {@code $$field$converted_to$ip}
 * but still referring to the same underlying field.
 */
public class FieldAttribute extends TypedAttribute {

    /**
     * A field name, as found in the mapping. Includes the whole path from the root of the document.
     * Implemented as a wrapper around {@link String} to distinguish from the attribute name (which sometimes differs!) at compile time.
     */
    public record FieldName(String string) {};

    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Attribute.class,
        "FieldAttribute",
        FieldAttribute::readFrom
    );

    private final String parentName;
    private final EsField field;
    protected FieldName lazyFieldName;

    public FieldAttribute(Source source, String name, EsField field) {
        this(source, null, name, field);
    }

    public FieldAttribute(Source source, @Nullable String parentName, String name, EsField field) {
        this(source, parentName, name, field, Nullability.TRUE, null, false);
    }

    public FieldAttribute(Source source, @Nullable String parentName, String name, EsField field, boolean synthetic) {
        this(source, parentName, name, field, Nullability.TRUE, null, synthetic);
    }

    public FieldAttribute(
        Source source,
        @Nullable String parentName,
        String name,
        EsField field,
        Nullability nullability,
        @Nullable NameId id,
        boolean synthetic
    ) {
        this(source, parentName, name, field.getDataType(), field, nullability, id, synthetic);
    }

    /**
     * Used only for testing. Do not use this otherwise, as an explicitly set type will be ignored the next time this FieldAttribute is
     * {@link FieldAttribute#clone}d.
     */
    FieldAttribute(
        Source source,
        @Nullable String parentName,
        String name,
        DataType type,
        EsField field,
        Nullability nullability,
        @Nullable NameId id,
        boolean synthetic
    ) {
        super(source, name, type, nullability, id, synthetic);
        this.parentName = parentName;
        this.field = field;
    }

    @Deprecated
    /**
     * Old constructor from when this had a qualifier string. Still needed to not break serialization.
     */
    private FieldAttribute(
        Source source,
        @Nullable String parentName,
        String name,
        DataType type,
        EsField field,
        @Nullable String qualifier,
        Nullability nullability,
        @Nullable NameId id,
        boolean synthetic
    ) {
        this(source, parentName, name, type, field, nullability, id, synthetic);
    }

    private FieldAttribute(StreamInput in) throws IOException {
        /*
         * The funny casting dance with `(StreamInput & PlanStreamInput) in` is required
         * because we're in esql-core here and the real PlanStreamInput is in
         * esql-proper. And because NamedWriteableRegistry.Entry needs StreamInput,
         * not a PlanStreamInput. And we need PlanStreamInput to handle Source
         * and NameId. This should become a hard cast when we move everything out
         * of esql-core.
         */
        this(
            Source.readFrom((StreamInput & PlanStreamInput) in),
            readParentName(in),
            readCachedStringWithVersionCheck(in),
            DataType.readFrom(in),
            EsField.readFrom(in),
            in.readOptionalString(),
            in.readEnum(Nullability.class),
            NameId.readFrom((StreamInput & PlanStreamInput) in),
            in.readBoolean()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (((PlanStreamOutput) out).writeAttributeCacheHeader(this)) {
            Source.EMPTY.writeTo(out);
            writeParentName(out);
            writeCachedStringWithVersionCheck(out, name());
            dataType().writeTo(out);
            field.writeTo(out);
            // We used to write the qualifier here. We can still do if needed in the future.
            out.writeOptionalString(null);
            out.writeEnum(nullable());
            id().writeTo(out);
            out.writeBoolean(synthetic());
        }
    }

    public static FieldAttribute readFrom(StreamInput in) throws IOException {
        return ((PlanStreamInput) in).readAttributeWithCache(FieldAttribute::new);
    }

    private void writeParentName(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_FIELD_ATTRIBUTE_PARENT_SIMPLIFIED)) {
            ((PlanStreamOutput) out).writeOptionalCachedString(parentName);
        } else {
            // Previous versions only used the parent field attribute to retrieve the parent's name, so we can use just any
            // fake FieldAttribute here as long as the name is correct.
            FieldAttribute fakeParent = parentName() == null ? null : new FieldAttribute(Source.EMPTY, parentName(), field());
            out.writeOptionalWriteable(fakeParent);
        }
    }

    private static String readParentName(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_FIELD_ATTRIBUTE_PARENT_SIMPLIFIED)) {
            return ((PlanStreamInput) in).readOptionalCachedString();
        }

        FieldAttribute parent = in.readOptionalWriteable(FieldAttribute::readFrom);
        return parent == null ? null : parent.name();
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<FieldAttribute> info() {
        return NodeInfo.create(this, FieldAttribute::new, parentName, name(), field, nullable(), id(), synthetic());
    }

    public String parentName() {
        return parentName;
    }

    /**
     * The full name of the field in the index, including all parent fields. E.g. {@code parent.subfield.this_field}.
     */
    public FieldName fieldName() {
        if (lazyFieldName == null) {
            // Before 8.15, the field name was the same as the attribute's name.
            // On later versions, the attribute can be renamed when creating synthetic attributes.
            // Because until 8.15, we couldn't set `synthetic` to true due to a bug, in that version such FieldAttributes are marked by
            // their
            // name starting with `$$`.
            if ((synthetic() || name().startsWith(SYNTHETIC_ATTRIBUTE_NAME_PREFIX)) == false) {
                lazyFieldName = new FieldName(name());
            }
            lazyFieldName = new FieldName(Strings.hasText(parentName) ? parentName + "." + field.getName() : field.getName());
        }
        return lazyFieldName;
    }

    public EsField.Exact getExactInfo() {
        return field.getExactInfo();
    }

    public FieldAttribute exactAttribute() {
        EsField exactField = field.getExactField();
        if (exactField.equals(field) == false) {
            return innerField(exactField);
        }
        return this;
    }

    private FieldAttribute innerField(EsField type) {
        return new FieldAttribute(source(), name(), name() + "." + type.getName(), type, nullable(), id(), synthetic());
    }

    @Override
    protected Attribute clone(Source source, String name, DataType type, Nullability nullability, NameId id, boolean synthetic) {
        // Ignore `type`, this must be the same as the field's type.
        return new FieldAttribute(source, parentName, name, field, nullability, id, synthetic);
    }

    @Override
    public Attribute withDataType(DataType type) {
        throw new UnsupportedOperationException("FieldAttribute obtains its type from the contained EsField.");
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), parentName, field);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
            && Objects.equals(parentName, ((FieldAttribute) obj).parentName)
            && Objects.equals(field, ((FieldAttribute) obj).field);
    }

    @Override
    protected String label() {
        return "f";
    }

    public EsField field() {
        return field;
    }
}
