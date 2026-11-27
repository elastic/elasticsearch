/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.capabilities.UnresolvedException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.LeafExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.io.IOException;
import java.util.Objects;

/**
 * A query parameter that holds the value reserved to stand for "Any" (the {@code param_any_value} setting), in a place where the
 * parameter would be an expression: a value, or a field if the parameter is a name, like {@code ??f} or an {@code identifier}.
 * <p>
 * It is never resolved, so its parents are not either: type resolution, implicit casting, folding and the optimizer never see
 * it, and it can't reach the execution or be serialized. What is not replaced by a rule of the analyzer is reported by the
 * {@code Verifier}, which reports every {@link Unresolvable} expression.
 */
public class AnyParam extends LeafExpression implements Unresolvable {

    private final String parameterText;
    private final String reservedValue;
    private final boolean field;
    @Nullable
    private final String reason;

    /**
     * @param parameterText how the parameter is written in the query, like {@code ?os} or {@code ??g}
     * @param reservedValue the value of the parameter, as reserved by the setting
     * @param field whether the parameter stands for a field rather than a value
     * @param reason why it can't be used where it is, or {@code null} for the general message
     */
    public AnyParam(Source source, String parameterText, String reservedValue, boolean field, @Nullable String reason) {
        super(source);
        this.parameterText = parameterText;
        this.reservedValue = reservedValue;
        this.field = field;
        this.reason = reason;
    }

    public AnyParam(Source source, String parameterText, String reservedValue, boolean field) {
        this(source, parameterText, reservedValue, field, null);
    }

    public String parameterText() {
        return parameterText;
    }

    /** Whether the parameter stands for a field, as {@code ??f} does, rather than for a value. */
    public boolean field() {
        return field;
    }

    public AnyParam withReason(String reason) {
        return new AnyParam(source(), parameterText, reservedValue, field, reason);
    }

    /**
     * The message of the error for a parameter with the reserved value in a place where it isn't supported.
     *
     * @param where what is wrong about the position, as a continuation of "can't be used"
     */
    public static String errorMessage(String parameterText, String reservedValue, String where) {
        return Strings.format(
            "Query parameter [%s] has the value [%s], which stands for \"Any\" and can't be used %s",
            parameterText,
            reservedValue,
            where
        );
    }

    @Override
    public String unresolvedMessage() {
        return errorMessage(parameterText, reservedValue, reason == null ? "here" : reason);
    }

    /** The method of {@link Expression} wins over the default of {@link Unresolvable}, so it has to be repeated. */
    @Override
    public boolean resolved() {
        return false;
    }

    @Override
    public boolean foldable() {
        return false;
    }

    /** Folding it is a bug of the code that tried to, but it has to be a 400 anyway: the query is what is wrong. */
    @Override
    public Object fold(FoldContext ctx) {
        throw new ParsingException(source(), unresolvedMessage());
    }

    @Override
    public Nullability nullable() {
        throw new UnresolvedException("nullable", this);
    }

    @Override
    public DataType dataType() {
        throw new UnresolvedException("data type", this);
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, AnyParam::new, parameterText, reservedValue, field, reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parameterText, reservedValue, field, reason);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AnyParam other = (AnyParam) obj;
        return field == other.field
            && Objects.equals(parameterText, other.parameterText)
            && Objects.equals(reservedValue, other.reservedValue)
            && Objects.equals(reason, other.reason);
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + "Any[" + parameterText + "]";
    }
}
