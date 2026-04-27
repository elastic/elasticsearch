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
public class MultiTypeEsField2 extends EsField implements UnionTypeEsField {

    public static final TransportVersion ESQL_MULTI_TYPE_ES_FIELD_2 = TransportVersion.fromName("esql_multi_type_es_field_2");

    private final Map<String, Expression> typeToConversionExpressions;

    @Nullable
    private final Expression unmappedConversionExpression;

    public MultiTypeEsField2(
        String name,
        DataType dataType,
        boolean aggregatable,
        Map<String, Expression> typeToConversionExpressions,
        TimeSeriesFieldType timeSeriesFieldType,
        @Nullable Expression unmappedConversionExpression
    ) {
        super(name, dataType, Map.of(), aggregatable, timeSeriesFieldType);
        this.typeToConversionExpressions = typeToConversionExpressions;
        this.unmappedConversionExpression = unmappedConversionExpression;
    }

    protected MultiTypeEsField2(StreamInput in) throws IOException {
        this(
            ((PlanStreamInput) in).readCachedString(),
            DataType.readFrom(in),
            in.readBoolean(),
            in.readImmutableMap(i -> i.readNamedWriteable(Expression.class)),
            readTimeSeriesFieldType(in),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        ((PlanStreamOutput) out).writeCachedString(getName());
        getDataType().writeTo(out);
        out.writeBoolean(isAggregatable());
        out.writeMap(typeToConversionExpressions, (o, v) -> out.writeNamedWriteable(v));
        writeTimeSeriesFieldType(out);
        out.writeOptionalNamedWriteable(unmappedConversionExpression);
    }

    @Override
    public String getWriteableName(TransportVersion transportVersion) {
        return "MultiTypeEsField2";
    }

    @Override
    public String getNodeStringName() {
        return "MultiTypeEsField2";
    }

    public Map<String, Expression> getTypeToConversionExpressions() {
        return typeToConversionExpressions;
    }

    /**
     * Returns the conversion expression to apply for the given source {@link DataType}, or {@code null}
     * if no conversion is registered for that type. Callers should fall back to
     * {@link #getUnmappedConversionExpression()} when the field is unmapped in the local index.
     */
    public @Nullable Expression getConversionExpressionForType(DataType type) {
        return typeToConversionExpressions.get(type.typeName());
    }

    @Override
    public @Nullable Expression getUnmappedConversionExpression() {
        return unmappedConversionExpression;
    }

    public MultiTypeEsField2 withUnmappedConversionExpression(@Nullable Expression unmappedConversionExpression) {
        return new MultiTypeEsField2(
            getName(),
            getDataType(),
            isAggregatable(),
            typeToConversionExpressions,
            getTimeSeriesFieldType(),
            unmappedConversionExpression
        );
    }

    /**
     * Build a {@link MultiTypeEsField2} from the per-type resolutions previously computed against an
     * {@link InvalidMappedField}. Only types present in {@code imf.getTypesToIndices()} for which a
     * conversion was supplied are included.
     */
    public static MultiTypeEsField2 resolveFrom(
        InvalidMappedField imf,
        Map<String, Expression> typesToConversionExpressions,
        @Nullable Expression unmappedConversionExpression
    ) {
        Map<String, Set<String>> typesToIndices = imf.getTypesToIndices();
        DataType resolvedDataType = DataType.UNSUPPORTED;
        Map<String, Expression> filtered = new HashMap<>();
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
            filtered.put(typeName, convertExpr);
        }
        if (resolvedDataType == DataType.UNSUPPORTED && unmappedConversionExpression != null) {
            resolvedDataType = unmappedConversionExpression.dataType();
        }
        return new MultiTypeEsField2(
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
        if (obj instanceof MultiTypeEsField2 other) {
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
