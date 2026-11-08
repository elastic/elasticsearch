/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Attribute based on a reference to an expression.
 */
public final class ReferenceAttribute extends TypedAttribute implements AnalyzedTextExpression, UnionTypedExpression {
    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Attribute.class,
        "ReferenceAttribute",
        ReferenceAttribute::readFrom
    );

    private static final TransportVersion ESQL_QUALIFIERS_IN_ATTRIBUTES = TransportVersion.fromName("esql_qualifiers_in_attributes");
    private static final TransportVersion ESQL_TO_TEXT_VALUES_ANALYZER = TransportVersion.fromName("esql_to_text_values_analyzer");

    private final String valuesAnalyzer;
    private final List<DataType> legTypes;

    @Deprecated
    /**
     * Only used for tests
     */
    public ReferenceAttribute(Source source, String name, DataType dataType) {
        this(source, null, name, dataType, Nullability.FALSE, null, false);
    }

    public ReferenceAttribute(Source source, @Nullable String qualifier, String name, DataType dataType) {
        this(source, qualifier, name, dataType, Nullability.FALSE, null, false);
    }

    public ReferenceAttribute(
        Source source,
        @Nullable String qualifier,
        String name,
        DataType dataType,
        Nullability nullability,
        @Nullable NameId id,
        boolean synthetic
    ) {
        this(source, qualifier, name, dataType, nullability, id, synthetic, null);
    }

    /**
     * @param valuesAnalyzer the values (index-time) analyzer declared for the referenced runtime text column via
     *                       {@code TO_TEXT}'s {@code analyzer} option, or {@code null} when none was declared. Plays
     *                       the role the mapping's {@code analyzer} plays for an indexed text field, so full-text
     *                       functions consuming the reference analyze its values consistently.
     */
    public ReferenceAttribute(
        Source source,
        @Nullable String qualifier,
        String name,
        DataType dataType,
        Nullability nullability,
        @Nullable NameId id,
        boolean synthetic,
        @Nullable String valuesAnalyzer
    ) {
        this(source, qualifier, name, dataType, nullability, id, synthetic, valuesAnalyzer, null);
    }

    /**
     * @param legTypes the types this reference's values can have, in the canonical leg order a tag indexes, when the
     *                 type is {@link DataType#UNION}; {@code null} for every other type. Required for a union and
     *                 refused for anything else, because the leg list is half of what {@code UNION} means: a value
     *                 carries a tag and the tag is an index into this list, so a reference holding the type without
     *                 the list holds a column nothing can read. Making that unconstructable is the nearest an enum
     *                 type can come to making it unrepresentable - see {@link UnionTypedExpression}'s TODO.
     */
    public ReferenceAttribute(
        Source source,
        @Nullable String qualifier,
        String name,
        DataType dataType,
        Nullability nullability,
        @Nullable NameId id,
        boolean synthetic,
        @Nullable String valuesAnalyzer,
        @Nullable List<DataType> legTypes
    ) {
        super(source, qualifier, name, dataType, nullability, id, synthetic);
        if ((dataType == DataType.UNION) != (legTypes != null)) {
            throw new IllegalArgumentException(
                dataType == DataType.UNION
                    ? "[" + name + "] is a union and must carry its leg types"
                    : "[" + name + "] is [" + dataType.typeName() + "] and cannot carry leg types, got " + legTypes
            );
        }
        this.valuesAnalyzer = valuesAnalyzer;
        this.legTypes = legTypes == null ? null : List.copyOf(legTypes);
    }

    @Override
    public List<DataType> unionLegTypes() {
        return legTypes;
    }

    @Override
    @Nullable
    public String valuesAnalyzer() {
        return valuesAnalyzer;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (((PlanStreamOutput) out).writeAttributeCacheHeader(this)) {
            Source.EMPTY.writeTo(out);
            out.writeString(name());
            dataType().writeTo(out);
            checkAndSerializeQualifier((PlanStreamOutput) out, out.getTransportVersion());
            if (out.getTransportVersion().supports(ESQL_QUALIFIERS_IN_ATTRIBUTES) == false) {
                // We used to always serialize a null qualifier here, so do the same for bwc.
                out.writeOptionalString(null);
            }
            nullable().writeTo(out);
            id().writeTo(out);
            out.writeBoolean(synthetic());
            if (legTypes != null) {
                // No version check: DataType.UNION#writeTo has already refused a peer that cannot read the type, so
                // anything still here can read the legs too. The type is the gate for its own parameter.
                out.writeCollection(legTypes, (o, t) -> t.writeTo(o));
            }
            if (out.getTransportVersion().supports(ESQL_TO_TEXT_VALUES_ANALYZER)) {
                out.writeOptionalString(valuesAnalyzer);
            } else if (valuesAnalyzer != null) {
                // Writing the reference without its analyzer would silently change how the column's values are
                // analyzed. IllegalArgumentException returns a 400 to the user, which is what we want here.
                throw new IllegalArgumentException(
                    "["
                        + name()
                        + "] with a values analyzer is not supported in peer node's version ["
                        + out.getTransportVersion()
                        + "]. Upgrade to version ["
                        + ESQL_TO_TEXT_VALUES_ANALYZER
                        + "] or newer."
                );
            }
        }
    }

    public static ReferenceAttribute readFrom(StreamInput in) throws IOException {
        return ((PlanStreamInput) in).readAttributeWithCache(ReferenceAttribute::innerReadFrom);
    }

    private static ReferenceAttribute innerReadFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        // We could cache this if we wanted to.
        String name = in.readString();
        DataType dataType = DataType.readFrom(in);
        String qualifier = readQualifier((PlanStreamInput) in, in.getTransportVersion());
        if (in.getTransportVersion().supports(ESQL_QUALIFIERS_IN_ATTRIBUTES) == false) {
            in.readOptionalString();
        }
        Nullability nullability = Nullability.readFrom(in);
        NameId id = NameId.readFrom((PlanStreamInput) in);
        boolean synthetic = in.readBoolean();
        List<DataType> legTypes = dataType == DataType.UNION ? in.readCollectionAsImmutableList(DataType::readFrom) : null;
        String valuesAnalyzer = in.getTransportVersion().supports(ESQL_TO_TEXT_VALUES_ANALYZER) ? in.readOptionalString() : null;

        return new ReferenceAttribute(source, qualifier, name, dataType, nullability, id, synthetic, valuesAnalyzer, legTypes);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Attribute clone(
        Source source,
        String qualifier,
        String name,
        DataType dataType,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        // clone's signature is fixed by Attribute, so the values analyzer and the leg types are always carried over
        // from this instance; the withName/withId/... helpers routing through clone cannot silently drop either. That
        // also means withDataType cannot turn a reference into a union, or a union into anything else, without the
        // constructor refusing it - which is the loud half of the invariant.
        return new ReferenceAttribute(source, qualifier, name, dataType, nullability, id, synthetic, valuesAnalyzer, legTypes);
    }

    @Override
    protected NodeInfo<ReferenceAttribute> info() {
        return NodeInfo.create(
            this,
            ReferenceAttribute::new,
            qualifier(),
            name(),
            dataType(),
            nullable(),
            id(),
            synthetic(),
            valuesAnalyzer,
            legTypes
        );
    }

    @Override
    protected int innerHashCode(boolean ignoreIds) {
        return Objects.hash(super.innerHashCode(ignoreIds), valuesAnalyzer, legTypes);
    }

    @Override
    protected boolean innerEquals(Object o, boolean ignoreIds) {
        var other = (ReferenceAttribute) o;
        return super.innerEquals(other, ignoreIds)
            && Objects.equals(valuesAnalyzer, other.valuesAnalyzer)
            && Objects.equals(legTypes, other.legTypes);
    }

    @Override
    protected String label() {
        return "r";
    }

    @Override
    public boolean isDimension() {
        return false;
    }

    @Override
    public boolean isMetric() {
        return false;
    }
}
