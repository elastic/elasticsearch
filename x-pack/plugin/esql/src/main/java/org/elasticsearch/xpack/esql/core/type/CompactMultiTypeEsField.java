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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/** Memory-efficient variant of {@link MultiTypeEsField} that stores the per-source-type conversion instead of per-index. */
public final class CompactMultiTypeEsField extends UnionTypeEsField {
    public static final TransportVersion CompactMultiTypeEsField = TransportVersion.fromName("compact_multi_type_es_field");

    // TODO these Expressions should be an AbstractConvertFunction.
    private final Map<DataType, Expression> typeToConversionExpressions;

    /**
     * If this is not {@code null}, then this expression should be used to convert the field value in case the field is not mapped in an
     * index from {@link DataType#KEYWORD} to the target type.
     */
    @Nullable
    private final Expression unmappedConversionExpression;

    public CompactMultiTypeEsField(
        String name,
        DataType dataType,
        boolean aggregatable,
        Map<DataType, Expression> typeToConversionExpressions,
        TimeSeriesFieldType timeSeriesFieldType,
        @Nullable Expression unmappedConversionExpression
    ) {
        super(name, dataType, Map.of(), aggregatable, timeSeriesFieldType);
        this.typeToConversionExpressions = typeToConversionExpressions;
        this.unmappedConversionExpression = unmappedConversionExpression;
    }

    CompactMultiTypeEsField(StreamInput in) throws IOException {
        this(
            ((PlanStreamInput) in).readCachedString(),
            DataType.readFrom(in),
            in.readBoolean(),
            in.readImmutableMap(DataType::readFrom, i -> i.readNamedWriteable(Expression.class)),
            readTimeSeriesFieldType(in),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        ((PlanStreamOutput) out).writeCachedString(getName());
        getDataType().writeTo(out);
        out.writeBoolean(isAggregatable());
        out.writeMap(typeToConversionExpressions, (o, k) -> k.writeTo(o), StreamOutput::writeNamedWriteable);
        writeTimeSeriesFieldType(out);
        out.writeOptionalNamedWriteable(unmappedConversionExpression);
    }

    @Override
    public String getWriteableName(TransportVersion transportVersion) {
        return getNodeStringName();
    }

    @Override
    public String getNodeStringName() {
        return "CompactMultiTypeEsField";
    }

    public Map<DataType, Expression> getTypeToConversionExpressions() {
        return typeToConversionExpressions;
    }

    @Override
    public Collection<Expression> getConversionExpressions() {
        return typeToConversionExpressions.values();
    }

    @Override
    public UnionTypeEsField rewrapWithCast(Expression convertExpression) {
        return new CompactMultiTypeEsField(
            getName(),
            convertExpression.dataType(),
            isAggregatable(),
            UnionTypeEsField.replaceChildrenWithExpressionField(typeToConversionExpressions, convertExpression),
            getTimeSeriesFieldType(),
            unmappedConversionExpression
        );
    }

    @Override
    public UnionTypeEsField withPotentiallyUnmappedExpression(Expression unmappedExpression) {
        return new CompactMultiTypeEsField(
            getName(),
            getDataType(),
            isAggregatable(),
            typeToConversionExpressions,
            getTimeSeriesFieldType(),
            unmappedExpression
        );
    }

    /**
     * Returns the conversion expression to apply for the given source {@link DataType}, or {@code null}
     * if no conversion is registered for that type.
     */
    public @Nullable Expression getConversionExpressionForType(DataType type) {
        return typeToConversionExpressions.get(type);
    }

    @Override
    public @Nullable Expression getUnmappedConversionExpression() {
        return unmappedConversionExpression;
    }

    public static CompactMultiTypeEsField resolveFrom(
        TypeConflictedField tcf,
        Map<String, Expression> typesToConversionExpressions,
        @Nullable Expression unmappedConversionExpression
    ) {
        UnionTypeEsField.Resolution resolution = UnionTypeEsField.resolve(tcf, typesToConversionExpressions);
        DataType resolvedDataType = resolution.resolvedDataType() == DataType.UNSUPPORTED && unmappedConversionExpression != null
            ? unmappedConversionExpression.dataType()
            : resolution.resolvedDataType();
        return new CompactMultiTypeEsField(
            tcf.getName(),
            resolvedDataType,
            false,
            resolution.typeToExpr(),
            tcf.getTimeSeriesFieldType(),
            unmappedConversionExpression
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        if (obj instanceof CompactMultiTypeEsField other) {
            return typeToConversionExpressions.equals(other.typeToConversionExpressions)
                && Objects.equals(unmappedConversionExpression, other.unmappedConversionExpression);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), typeToConversionExpressions, unmappedConversionExpression);
    }

    @Override
    public String toString() {
        return Strings.format("%s (%s, %s)", super.toString(), typeToConversionExpressions, unmappedConversionExpression);
    }
}
