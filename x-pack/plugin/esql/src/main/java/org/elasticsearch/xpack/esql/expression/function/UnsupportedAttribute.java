/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.core.util.PlanStreamOutput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.util.PlanStreamInput.readCachedStringWithVersionCheck;
import static org.elasticsearch.xpack.esql.core.util.PlanStreamOutput.writeCachedStringWithVersionCheck;

/**
 * Unsupported attribute meaning an attribute that has been found yet cannot be used (hence why UnresolvedAttribute
 * cannot be used) expect in special conditions (currently only in projections to allow it to flow through
 * the engine).
 * As such the field is marked as unresolved (so the verifier can pick up its usage outside project).
 */
public final class UnsupportedAttribute extends FieldAttribute implements Unresolvable {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Attribute.class,
        "UnsupportedAttribute",
        UnsupportedAttribute::readFrom
    );
    public static final NamedWriteableRegistry.Entry NAMED_EXPRESSION_ENTRY = new NamedWriteableRegistry.Entry(
        NamedExpression.class,
        ENTRY.name,
        UnsupportedAttribute::readFrom
    );
    public static final NamedWriteableRegistry.Entry EXPRESSION_ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        ENTRY.name,
        UnsupportedAttribute::readFrom
    );

    private final boolean hasCustomMessage; // TODO remove me and just use message != null?
    private final String message;

    private static String errorMessage(String name, UnsupportedEsField field) {
        return "Cannot use field [" + name + "] with unsupported type [" + String.join(",", field.getOriginalTypes()) + "]";
    }

    public UnsupportedAttribute(Source source, String name, UnsupportedEsField field) {
        this(source, name, field, null);
    }

    public UnsupportedAttribute(Source source, String name, UnsupportedEsField field, @Nullable String customMessage) {
        this(source, name, field, customMessage, null);
    }

    public UnsupportedAttribute(Source source, String name, UnsupportedEsField field, @Nullable String customMessage, @Nullable NameId id) {
        super(source, null, name, field, Nullability.TRUE, id, false);
        this.hasCustomMessage = customMessage != null;
        this.message = customMessage == null ? errorMessage(name(), field) : customMessage;
    }

    private UnsupportedAttribute(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            readCachedStringWithVersionCheck(in),
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_2) ? EsField.readFrom(in) : new UnsupportedEsField(in),
            in.readOptionalString(),
            NameId.readFrom((PlanStreamInput) in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (((PlanStreamOutput) out).writeAttributeCacheHeader(this)) {
            Source.EMPTY.writeTo(out);
            writeCachedStringWithVersionCheck(out, name());
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_2)) {
                field().writeTo(out);
            } else {
                field().writeContent(out);
            }
            out.writeOptionalString(hasCustomMessage ? message : null);
            id().writeTo(out);
        }
    }

    public static UnsupportedAttribute readFrom(StreamInput in) throws IOException {
        return ((PlanStreamInput) in).readAttributeWithCache(UnsupportedAttribute::new);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public boolean resolved() {
        return false;
    }

    @Override
    public UnsupportedEsField field() {
        return (UnsupportedEsField) super.field();
    }

    @Override
    public String fieldName() {
        // The super fieldName uses parents to compute the path; this class ignores parents, so we need to rely on the name instead.
        // Using field().getName() would be wrong: for subfields like parent.subfield that would return only the last part, subfield.
        return name();
    }

    @Override
    protected NodeInfo<FieldAttribute> info() {
        return NodeInfo.create(this, UnsupportedAttribute::new, name(), field(), hasCustomMessage ? message : null, id());
    }

    @Override
    protected Attribute clone(Source source, String name, DataType type, Nullability nullability, NameId id, boolean synthetic) {
        return new UnsupportedAttribute(source, name, field(), hasCustomMessage ? message : null, id);
    }

    protected String label() {
        return "!";
    }

    @Override
    public String toString() {
        return "!" + name();
    }

    @Override
    public String nodeString() {
        return toString();
    }

    @Override
    public String unresolvedMessage() {
        return message;
    }

    public boolean hasCustomMessage() {
        return hasCustomMessage;
    }

    @Override
    @SuppressWarnings("checkstyle:EqualsHashCode")// equals is implemented in parent. See innerEquals instead
    public int hashCode() {
        return Objects.hash(super.hashCode(), hasCustomMessage, message);
    }

    @Override
    protected boolean innerEquals(Object o) {
        var other = (UnsupportedAttribute) o;
        return super.innerEquals(other) && hasCustomMessage == other.hasCustomMessage && Objects.equals(message, other.message);
    }

    /**
     * This contains all the underlying ES types.
     * On a type conflict this will have many elements, some or all of which may be actually supported types.
     */
    public List<String> originalTypes() {
        return field().getOriginalTypes();
    }
}
