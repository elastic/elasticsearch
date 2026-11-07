/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A field whose sources do not agree on one type, kept as a union rather than erased to
 * {@link DataType#UNSUPPORTED}. It carries the types its values can have, as legs, in the order
 * {@link TypeConflictedField#unionLegTypes()} fixes.
 * <p>
 * The order is the whole of the contract between the block and the response: the block holds one sub-block per
 * leg in this order, and the response declares these types in this order, so a value's tag names the same type
 * on both sides. Neither side carries the order to the other, because both derive it from the leg types.
 * </p>
 * <p>
 * Not aggregatable, like the other multi-type fields: there is no single doc-values type to aggregate over.
 * </p>
 */
public class UnionEsField extends EsField {

    private final List<DataType> legTypes;

    public UnionEsField(String name, List<DataType> legTypes) {
        super(name, DataType.UNION, Map.of(), false, TimeSeriesFieldType.NONE);
        if (legTypes.size() < 2) {
            throw new IllegalArgumentException("a union field needs at least two legs, got " + legTypes);
        }
        this.legTypes = List.copyOf(legTypes);
    }

    /** The union of a conflicted field's types, in the canonical leg order. */
    public static UnionEsField from(TypeConflictedField conflicted) {
        return new UnionEsField(conflicted.getName(), conflicted.unionLegTypes());
    }

    UnionEsField(StreamInput in) throws IOException {
        this(((PlanStreamInput) in).readCachedString(), in.readCollectionAsImmutableList(DataType::readFrom));
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        ((PlanStreamOutput) out).writeCachedString(getName());
        out.writeCollection(legTypes, (o, type) -> type.writeTo(o));
    }

    @Override
    public String getWriteableName(TransportVersion transportVersion) {
        return getNodeStringName();
    }

    @Override
    public String getNodeStringName() {
        return "UnionEsField";
    }

    /**
     * Whether a conflicted field's types, in the canonical leg order, make a union this engine can carry.
     * <p>
     * Two legs at least, because a conflict whose types all widen to one type is not a conflict any more, and every leg a type the
     * block can hold.
     * </p>
     */
    public static boolean canUnion(List<DataType> legTypes) {
        if (legTypes.size() < 2) {
            return false;
        }
        for (DataType legType : legTypes) {
            if (isLegType(legType) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether a value of this type can sit in a leg. A leg is one conventional block, so the type has to be a single-valued scalar
     * whose element type is fixed by the type alone, and the physical counterpart of this list is
     * {@code UnionBlockBuilder#canHoldLeg}, which {@code UnionEsFieldTests} pins this against.
     */
    public static boolean isLegType(DataType type) {
        return switch (type) {
            case BOOLEAN, INTEGER, LONG, UNSIGNED_LONG, DOUBLE, KEYWORD, TEXT, DATETIME, DATE_NANOS, IP, VERSION, FLATTENED -> true;
            // Widened away by TypeConflictedField#unionLegTypes before a leg ever carries one.
            case SHORT, BYTE, FLOAT, HALF_FLOAT, SCALED_FLOAT -> false;
            // The element type depends on the field-extract preference rather than on the type, so the leg's physical type would not
            // be fixed by the leg order the response and the block both derive from these types.
            case GEO_POINT, CARTESIAN_POINT, GEO_SHAPE, CARTESIAN_SHAPE -> false;
            // No leg can hold these: either a block the union builder has no append for, or a value that is not one scalar.
            case COUNTER_LONG, COUNTER_INTEGER, COUNTER_DOUBLE, AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, TDIGEST, HISTOGRAM,
                DATE_RANGE, DOUBLE_RANGE, DENSE_VECTOR, PARTIAL_AGG -> false;
            // Not a type a mapping conflict offers as one of a field's value types.
            case UNSUPPORTED, UNION, NULL, OBJECT, SOURCE, DATE_PERIOD, TIME_DURATION, DOC_DATA_TYPE, TSID_DATA_TYPE, GEOHASH, GEOTILE,
                GEOHEX -> false;
        };
    }

    /** The types a value of this field can have, in the canonical leg order. */
    public List<DataType> legTypes() {
        return legTypes;
    }

    /** The leg a value of the given type belongs in, or -1 if this union has no leg for that type. */
    public int legOf(DataType type) {
        return legTypes.indexOf(type.widenSmallNumeric());
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && obj instanceof UnionEsField other && legTypes.equals(other.legTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), legTypes);
    }

    @Override
    public String toString() {
        return super.toString() + legTypes;
    }
}
