/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.qlcore.expression.function.scalar;

import org.elasticsearch.xpack.qlcore.expression.Expression;
import org.elasticsearch.xpack.qlcore.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.qlcore.expression.gen.script.Scripts;
import org.elasticsearch.xpack.qlcore.tree.Source;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public abstract class BinaryScalarFunction extends ScalarFunction {

    private final Expression left, right;

    protected BinaryScalarFunction(Source source, Expression left, Expression right) {
        super(source, Arrays.asList(left, right));
        this.left = left;
        this.right = right;
    }

    @Override
    public final BinaryScalarFunction replaceChildren(List<Expression> newChildren) {
        Expression newLeft = newChildren.get(0);
        Expression newRight = newChildren.get(1);

        return left.equals(newLeft) && right.equals(newRight) ? this : replaceChildren(newLeft, newRight);
    }

    protected abstract BinaryScalarFunction replaceChildren(Expression newLeft, Expression newRight);

    public Expression left() {
        return left;
    }

    public Expression right() {
        return right;
    }

    @Override
    public boolean foldable() {
        return left.foldable() && right.foldable();
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate leftScript = asScript(left());
        ScriptTemplate rightScript = asScript(right());

        return asScriptFrom(leftScript, rightScript);
    }

    protected ScriptTemplate asScriptFrom(ScriptTemplate leftScript, ScriptTemplate rightScript) {
        return Scripts.binaryMethod(Scripts.classPackageAsPrefix(getClass()), scriptMethodName(), leftScript, rightScript, dataType());
    }

    protected String scriptMethodName() {
        return getClass().getSimpleName().toLowerCase(Locale.ROOT);
    }
}
