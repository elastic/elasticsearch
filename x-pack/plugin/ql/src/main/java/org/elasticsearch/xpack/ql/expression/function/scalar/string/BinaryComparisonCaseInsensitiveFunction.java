/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Locale;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

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
        return DataTypes.BOOLEAN;
    }

    @Override
    public boolean foldable() {
        return left.foldable() && right.foldable();
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate leftScript = asScript(left);
        ScriptTemplate rightScript = asScript(right);

        return asScriptFrom(leftScript, rightScript);
    }

    protected ScriptTemplate asScriptFrom(ScriptTemplate leftScript, ScriptTemplate rightScript) {
        return new ScriptTemplate(
            format(
                Locale.ROOT,
                formatTemplate("%s.%s(%s,%s,%s)"),
                Scripts.classPackageAsPrefix(getClass()),
                scriptMethodName(),
                leftScript.template(),
                rightScript.template(),
                "{}"
            ),
            paramsBuilder().script(leftScript.params()).script(rightScript.params()).variable(isCaseInsensitive()).build(),
            dataType()
        );
    }

    protected String scriptMethodName() {
        String simpleName = getClass().getSimpleName();
        return Character.toLowerCase(simpleName.charAt(0)) + simpleName.substring(1);
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
