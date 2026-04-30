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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

// FIXME(gal, NOCOMMIT) Go over this javadocs
// FIXME(gal, NOCOMMIT) Reduce duplication with MultiTypeEsField
/**
 * Memory-efficient variant of {@link MultiTypeEsField} that stores the per-source-type conversion
 * expressions directly, rather than expanding them to one entry per index. Plus an optional
 * {@code unmappedConversionExpression} for indices in which the field is unmapped (treated as
 * {@link DataType#KEYWORD}). On a data node, the conversion expression is looked up by the field's
 * locally-resolved data type.
 *
 * <p>This is the on-the-wire successor to {@link MultiTypeEsField}; the analyzer falls back to the
 * legacy {@link MultiTypeEsField} when the cluster minimum transport version does not yet support
 * {@code esql_multi_type_es_field_2}.
 */
public class CompactMultiTypeEsField extends EsField implements UnionTypeEsField {
    // FIXME(gal, NOCOMMIT) rename
    public static final TransportVersion ESQL_MULTI_TYPE_ES_FIELD_2 = TransportVersion.fromName("esql_multi_type_es_field_2");

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

    protected CompactMultiTypeEsField(StreamInput in) throws IOException {
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
        out.writeMap(typeToConversionExpressions, (o, k) -> k.writeTo(o), (o, v) -> o.writeNamedWriteable(v));
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

    /**
     * Returns the conversion expression to apply for the given source {@link DataType}, or {@code null}
     * if no conversion is registered for that type. Callers should fall back to
     * {@link #getUnmappedConversionExpression()} when the field is unmapped in the local index.
     */
    public @Nullable Expression getConversionExpressionForType(DataType type) {
        return typeToConversionExpressions.get(type);
    }

    @Override
    public @Nullable Expression getUnmappedConversionExpression() {
        return unmappedConversionExpression;
    }

    /**
     * Build a {@link CompactMultiTypeEsField} from the per-type resolutions previously computed against a
     * {@link TypeConflictField}. Only types present in {@code imf.getTypesToIndices()} for which a
     * conversion was supplied are included.
     */
    public static CompactMultiTypeEsField resolveFrom(
        TypeConflictField imf,
        Map<String, Expression> typesToConversionExpressions,
        @Nullable Expression unmappedConversionExpression
    ) {
        Map<String, Set<String>> typesToIndices = imf.getTypesToIndices();
        DataType resolvedDataType = DataType.UNSUPPORTED;
        Map<DataType, Expression> filtered = new HashMap<>();
        for (String typeName : typesToIndices.keySet()) {
            Expression convertExpr = typesToConversionExpressions.get(typeName);
            if (convertExpr == null) {
                continue;
            }
            if (resolvedDataType == DataType.UNSUPPORTED) {
                resolvedDataType = convertExpr.dataType();
            } else if (resolvedDataType != convertExpr.dataType()) {
                throw new IllegalArgumentException("Resolved data type mismatch: " + resolvedDataType + " != " + convertExpr.dataType());
            }
            filtered.put(DataType.fromTypeName(typeName), convertExpr);
        }
        if (resolvedDataType == DataType.UNSUPPORTED && unmappedConversionExpression != null) {
            resolvedDataType = unmappedConversionExpression.dataType();
        }
        return new CompactMultiTypeEsField(
            imf.getName(),
            resolvedDataType,
            false,
            filtered,
            imf.getTimeSeriesFieldType(),
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
