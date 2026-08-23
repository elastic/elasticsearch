/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.blockloader.Warnings;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.TemporalityAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * A transport-safe, complete description of one field extraction operation.
 * <p>
 * The specification contains semantic choices that must be identical on the coordinator and data node. It does not contain live
 * shard objects or node-local tuning values. The target node binds the specification to its local mapping and planner settings.
 * <p>
 * New extraction operations belong in this protocol. The planner must use eager extraction when a target node cannot read the
 * required operation. It must not replace the specification with a lower-fidelity representation.
 */
public final class FieldExtractionSpec implements Writeable {
    private static final TransportVersion DIRECT_FETCH_EXTRACTION = TransportVersion.fromName("esql_fetch_boundary");

    /** The extraction operation that the target node must perform. */
    public enum Operation {
        /** Resolve a mapped field on each shard and load it without a field-specific conversion. */
        DIRECT
    }

    /** The result to produce when the target shard does not contain the field. */
    public enum MissingFieldPolicy {
        /** Produce a constant null value. */
        NULL
    }

    private final Operation operation;
    private final String fieldName;
    private final DataType dataType;
    private final ElementType elementType;
    private final MappedFieldType.FieldExtractPreference fieldExtractPreference;
    private final MissingFieldPolicy missingFieldPolicy;

    private FieldExtractionSpec(
        Operation operation,
        String fieldName,
        DataType dataType,
        ElementType elementType,
        MappedFieldType.FieldExtractPreference fieldExtractPreference,
        MissingFieldPolicy missingFieldPolicy
    ) {
        this.operation = Objects.requireNonNull(operation, "operation");
        this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
        this.dataType = Objects.requireNonNull(dataType, "dataType");
        this.elementType = Objects.requireNonNull(elementType, "elementType");
        this.fieldExtractPreference = Objects.requireNonNull(fieldExtractPreference, "fieldExtractPreference");
        this.missingFieldPolicy = Objects.requireNonNull(missingFieldPolicy, "missingFieldPolicy");
        validate();
    }

    /** Reads one specification from the transport stream. */
    public FieldExtractionSpec(StreamInput in) throws IOException {
        this(
            in.readEnum(Operation.class),
            in.readString(),
            DataType.readFrom(in.readString()),
            in.readEnum(ElementType.class),
            in.readEnum(MappedFieldType.FieldExtractPreference.class),
            in.readEnum(MissingFieldPolicy.class)
        );
    }

    /**
     * Builds the complete direct-extraction specification for an attribute, or returns empty when the attribute requires another
     * extraction operation.
     */
    public static Optional<FieldExtractionSpec> plan(Attribute attribute, MappedFieldType.FieldExtractPreference fieldExtractPreference) {
        if (attribute instanceof TimeSeriesMetadataAttribute || attribute instanceof TemporalityAttribute) {
            return Optional.empty();
        }
        String fieldName;
        if (attribute instanceof FieldAttribute fieldAttribute) {
            if (fieldAttribute.field() instanceof FunctionEsField
                || fieldAttribute.field() instanceof PotentiallyUnmappedKeywordEsField
                || fieldAttribute.field() instanceof UnionTypeEsField) {
                return Optional.empty();
            }
            fieldName = fieldAttribute.fieldName().string();
        } else if (attribute instanceof MetadataAttribute) {
            fieldName = attribute.name();
        } else {
            return Optional.empty();
        }
        if (supportsDirectDataType(attribute.dataType()) == false) {
            return Optional.empty();
        }
        return Optional.of(direct(fieldName, attribute.dataType(), fieldExtractPreference));
    }

    /** Builds a direct field extraction specification. */
    public static FieldExtractionSpec direct(
        String fieldName,
        DataType dataType,
        MappedFieldType.FieldExtractPreference fieldExtractPreference
    ) {
        if (supportsDirectDataType(dataType) == false) {
            throw new IllegalArgumentException("data type [" + dataType.typeName() + "] requires a specialized extraction operation");
        }
        return new FieldExtractionSpec(
            Operation.DIRECT,
            fieldName,
            dataType,
            PlannerUtils.toElementType(dataType, fieldExtractPreference),
            fieldExtractPreference,
            MissingFieldPolicy.NULL
        );
    }

    /** Builds a direct field extraction specification with no extraction preference. */
    public static FieldExtractionSpec direct(String fieldName, DataType dataType) {
        return direct(fieldName, dataType, MappedFieldType.FieldExtractPreference.NONE);
    }

    /** Whether the direct operation contains all extraction semantics required by this logical type. */
    public static boolean supportsDirectDataType(DataType dataType) {
        return switch (dataType) {
            case UNSUPPORTED, NULL, BOOLEAN, COUNTER_LONG, COUNTER_INTEGER, COUNTER_DOUBLE, LONG, INTEGER, UNSIGNED_LONG, DOUBLE, KEYWORD,
                TEXT, DATETIME, DATE_NANOS, DATE_RANGE, DOUBLE_RANGE, IP, VERSION, SOURCE, TSID_DATA_TYPE, AGGREGATE_METRIC_DOUBLE,
                EXPONENTIAL_HISTOGRAM, TDIGEST, HISTOGRAM, DENSE_VECTOR, FLATTENED -> true;
            // These mapped numeric types require widening before a direct extraction specification can describe them.
            case SHORT, BYTE, FLOAT, HALF_FLOAT, SCALED_FLOAT -> false;
            // Spatial extraction needs its own operation because the preference changes the physical representation.
            case GEO_POINT, CARTESIAN_POINT, CARTESIAN_SHAPE, GEO_SHAPE -> false;
            // These types are evaluator or execution values rather than independently loadable mapped fields.
            case OBJECT, DATE_PERIOD, TIME_DURATION, GEOHASH, GEOTILE, GEOHEX, DOC_DATA_TYPE, PARTIAL_AGG -> false;
        };
    }

    /**
     * Binds this semantic specification to one target shard.
     */
    public ValuesSourceReaderOperator.LoaderAndConverter bind(
        EsPhysicalOperationProviders.ShardContext shardContext,
        PlannerSettings plannerSettings,
        @Nullable Warnings warnings
    ) {
        return switch (operation) {
            case DIRECT -> switch (missingFieldPolicy) {
                case NULL -> ValuesSourceReaderOperator.load(
                    shardContext.blockLoader(
                        fieldName,
                        dataType == DataType.UNSUPPORTED,
                        fieldExtractPreference,
                        null,
                        warnings,
                        plannerSettings.blockLoaderSizeOrdinals(),
                        plannerSettings.blockLoaderSizeScript()
                    )
                );
            };
        };
    }

    /** Returns the extraction operation. */
    public Operation operation() {
        return operation;
    }

    /** Returns the mapped field name. */
    public String fieldName() {
        return fieldName;
    }

    /** Returns the logical result type. */
    public DataType dataType() {
        return dataType;
    }

    /** Returns the physical block element type. */
    public ElementType elementType() {
        return elementType;
    }

    /** Returns the requested source for field extraction. */
    public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
        return fieldExtractPreference;
    }

    /** Returns the behavior for a shard on which the field is not mapped. */
    public MissingFieldPolicy missingFieldPolicy() {
        return missingFieldPolicy;
    }

    /** Whether the target node can execute this extraction operation. */
    public boolean supports(TransportVersion transportVersion) {
        return switch (operation) {
            case DIRECT -> transportVersion.supports(DIRECT_FETCH_EXTRACTION);
        };
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(operation);
        out.writeString(fieldName);
        out.writeString(dataType.typeName());
        out.writeEnum(elementType);
        out.writeEnum(fieldExtractPreference);
        out.writeEnum(missingFieldPolicy);
    }

    private void validate() {
        switch (operation) {
            case DIRECT -> {
                if (supportsDirectDataType(dataType) == false) {
                    throw new IllegalArgumentException(
                        "direct extraction does not contain the semantics required by type [" + dataType.typeName() + "]"
                    );
                }
                ElementType expectedElementType = PlannerUtils.toElementType(dataType, fieldExtractPreference);
                if (elementType != expectedElementType) {
                    throw new IllegalArgumentException(
                        "direct extraction for type ["
                            + dataType.typeName()
                            + "] and preference ["
                            + fieldExtractPreference
                            + "] requires physical type ["
                            + expectedElementType
                            + "] but received ["
                            + elementType
                            + "]"
                    );
                }
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof FieldExtractionSpec == false) {
            return false;
        }
        FieldExtractionSpec other = (FieldExtractionSpec) obj;
        return operation == other.operation
            && fieldName.equals(other.fieldName)
            && dataType == other.dataType
            && elementType == other.elementType
            && fieldExtractPreference == other.fieldExtractPreference
            && missingFieldPolicy == other.missingFieldPolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(operation, fieldName, dataType, elementType, fieldExtractPreference, missingFieldPolicy);
    }

    @Override
    public String toString() {
        return operation
            + "["
            + fieldName
            + ":"
            + dataType.typeName()
            + "->"
            + elementType
            + ", preference="
            + fieldExtractPreference
            + "]";
    }
}
