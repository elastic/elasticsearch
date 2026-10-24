/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/** Shared type validation and transport for operations on time-series label sets. */
public abstract class TimeSeriesFunction extends EsqlScalarFunction {
    // Match the public signatures; representable types also include vectors, geometry, and internal engine values.
    private static final Set<DataType> LABEL_VALUE_TYPES = Set.of(
        DataType.KEYWORD,
        DataType.TEXT,
        DataType.BOOLEAN,
        DataType.INTEGER,
        DataType.LONG,
        DataType.UNSIGNED_LONG,
        DataType.DOUBLE,
        DataType.IP
    );

    protected TimeSeriesFunction(Source source, List<Expression> arguments) {
        super(source, arguments);
    }

    protected TimeSeriesFunction(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteableCollectionAsList(Expression.class));
    }

    public Expression metadata() {
        return children().getFirst();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteableCollection(children());
    }

    @Override
    protected final TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        // Null is the empty dictionary, including when supplied directly in an ES|QL expression.
        return isType(metadata(), type -> type == DataType.TIMESERIES_DATA_TYPE, sourceText(), ParamOrdinal.FIRST, "_timeseries").and(
            resolveArguments()
        );
    }

    protected abstract TypeResolution resolveArguments();

    protected final TypeResolution labelName(int index) {
        Expression name = children().get(index);
        ParamOrdinal ordinal = ParamOrdinal.fromIndex(index);
        TypeResolution resolution = isString(name, sourceText(), ordinal).and(isFoldable(name, sourceText(), ordinal));
        if (resolution.unresolved()) {
            return resolution;
        }
        return name.fold(FoldContext.small()) instanceof BytesRef bytes && bytes.length > 0
            ? TypeResolution.TYPE_RESOLVED
            : new TypeResolution("label names in [" + sourceText() + "] must be non-empty, single-valued strings");
    }

    /** TS_HASH accepts optional name/value pairs for promoted columns. */
    protected final TypeResolution namedValues() {
        if ((children().size() - 1) % 2 != 0) {
            return new TypeResolution("[" + sourceText() + "] expects label name/value pairs after the metadata argument");
        }
        var names = new HashSet<Object>();
        for (int i = 1; i < children().size(); i += 2) {
            TypeResolution resolution = labelName(i).and(labelValue(i + 1));
            if (resolution.unresolved()) {
                return resolution;
            }
            if (names.add(children().get(i).fold(FoldContext.small())) == false) {
                return new TypeResolution("[" + sourceText() + "] contains duplicate label names");
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    protected final TypeResolution labelValue(int index) {
        return isType(
            children().get(index),
            LABEL_VALUE_TYPES::contains,
            sourceText(),
            ParamOrdinal.fromIndex(index),
            "string",
            "boolean",
            "integer",
            "long",
            "unsigned_long",
            "double",
            "ip"
        );
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    public final boolean foldable() {
        return Expressions.foldable(children());
    }
}
