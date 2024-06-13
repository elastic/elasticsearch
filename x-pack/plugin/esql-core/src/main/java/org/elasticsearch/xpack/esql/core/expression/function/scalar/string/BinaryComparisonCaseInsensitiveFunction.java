/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.function.scalar.string;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Objects;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isStringAndExact;

public abstract class BinaryComparisonCaseInsensitiveFunction extends CaseInsensitiveScalarFunction {

    private final Expression left, right;

    protected BinaryComparisonCaseInsensitiveFunction(Source source, Expression left, Expression right, boolean caseInsensitive) {
        super(source, asList(left, right), caseInsensitive);
        this.left = left;
        this.right = right;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution sourceResolution = isStringAndExact(left, sourceText(), FIRST);
        if (sourceResolution.unresolved()) {
            return sourceResolution;
        }

        return isStringAndExact(right, sourceText(), SECOND);
    }

    public Expression left() {
        return left;
    }

    public Expression right() {
        return right;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public boolean foldable() {
        return left.foldable() && right.foldable();
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, isCaseInsensitive());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        BinaryComparisonCaseInsensitiveFunction other = (BinaryComparisonCaseInsensitiveFunction) obj;
        return Objects.equals(left, other.left)
            && Objects.equals(right, other.right)
            && Objects.equals(isCaseInsensitive(), other.isCaseInsensitive());
    }
}
