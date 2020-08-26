/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.function.scalar;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.Check;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public abstract class BinaryScalarFunction extends ScalarFunction {

    private static final int PKG_LENGTH = "org.elasticsearch.xpack.".length();
    private final Expression left, right;

    protected BinaryScalarFunction(Source source, Expression left, Expression right) {
        super(source, Arrays.asList(left, right));
        this.left = left;
        this.right = right;
    }

    @Override
    public final BinaryScalarFunction replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 2) {
            throw new IllegalArgumentException("expected [2] children but received [" + newChildren.size() + "]");
        }
        return replaceChildren(newChildren.get(0), newChildren.get(1));
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
        String prefix = getClass().getPackageName().substring(PKG_LENGTH);
        int index = prefix.indexOf('.');
        Check.isTrue(index > 0, "invalid package {}", prefix);
        return Scripts.binaryMethod("{" + prefix.substring(0, index) + "}", scriptMethodName(), leftScript, rightScript, dataType());
    }
    
    protected String scriptMethodName() {
        return getClass().getSimpleName().toLowerCase(Locale.ROOT);
    }
}
