/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

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

import static org.elasticsearch.TransportVersions.ESQL_FIELD_ATTRIBUTE_DROP_TYPE;
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
 * and {@code field::ip}, we'll generate 2 field attributes called {@code $$field$converted_to$keyword} and {@code $$field$converted_to$ip}
 * which still refer to the same underlying index field.
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
        super(source, name, field.getDataType(), nullability, id, synthetic);
        this.parentName = parentName;
        this.field = field;
    }

    private static FieldAttribute innerReadFrom(StreamInput in) throws IOException {
        /*
         * The funny casting dance with `(StreamInput & PlanStreamInput) in` is required
         * because we're in esql-core here and the real PlanStreamInput is in
         * esql-proper. And because NamedWriteableRegistry.Entry needs StreamInput,
         * not a PlanStreamInput. And we need PlanStreamInput to handle Source
         * and NameId. This should become a hard cast when we move everything out
         * of esql-core.
         */
        Source source = Source.readFrom((StreamInput & PlanStreamInput) in);
        String parentName = ((PlanStreamInput) in).readOptionalCachedString();
        String name = readCachedStringWithVersionCheck(in);
        if (in.getTransportVersion().before(ESQL_FIELD_ATTRIBUTE_DROP_TYPE)) {
            DataType.readFrom(in);
        }
        EsField field = EsField.readFrom(in);
        if (in.getTransportVersion().before(ESQL_FIELD_ATTRIBUTE_DROP_TYPE)) {
            in.readOptionalString();
        }
        Nullability nullability = in.readEnum(Nullability.class);
        NameId nameId = NameId.readFrom((StreamInput & PlanStreamInput) in);
        boolean synthetic = in.readBoolean();
        return new FieldAttribute(source, parentName, name, field, nullability, nameId, synthetic);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (((PlanStreamOutput) out).writeAttributeCacheHeader(this)) {
            Source.EMPTY.writeTo(out);
            ((PlanStreamOutput) out).writeOptionalCachedString(parentName);
            writeCachedStringWithVersionCheck(out, name());
            if (out.getTransportVersion().before(ESQL_FIELD_ATTRIBUTE_DROP_TYPE)) {
                dataType().writeTo(out);
            }
            field.writeTo(out);
            if (out.getTransportVersion().before(ESQL_FIELD_ATTRIBUTE_DROP_TYPE)) {
                // We used to write the qualifier here. We can still do if needed in the future.
                out.writeOptionalString(null);
            }
            out.writeEnum(nullable());
            id().writeTo(out);
            out.writeBoolean(synthetic());
        }
    }

    public static FieldAttribute readFrom(StreamInput in) throws IOException {
        return ((PlanStreamInput) in).readAttributeWithCache(FieldAttribute::innerReadFrom);
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

    /**
     * The name of the attribute. Can deviate from the field name e.g. in case of union types. For the physical field name, use
     * {@link FieldAttribute#fieldName()}.
     */
    @Override
    public String name() {
        return super.name();
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
        return new FieldAttribute(source(), fieldName().string, name() + "." + type.getName(), type, nullable(), id(), synthetic());
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
    @SuppressWarnings("checkstyle:EqualsHashCode")// equals is implemented in parent. See innerEquals instead
    public int hashCode() {
        return Objects.hash(super.hashCode(), parentName, field);
    }

    @Override
    protected boolean innerEquals(Object o) {
        var other = (FieldAttribute) o;
        return super.innerEquals(other) && Objects.equals(parentName, other.parentName) && Objects.equals(field, other.field);
    }

    @Override
    protected String label() {
        return "f";
    }

    public EsField field() {
        return field;
    }
}
