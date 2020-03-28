/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.stringcontains;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static java.lang.String.format;
import static org.elasticsearch.xpack.eql.expression.function.scalar.stringcontains.StringContainsFunctionProcessor.doProcess;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isBoolean;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * EQL specific stringContains function.
 * https://eql.readthedocs.io/en/latest/query-guide/functions.html#stringContains
 * stringContains(a, b)
 * Returns true if b is a substring of a
 */
public class StringContains extends ScalarFunction implements OptionalArgument {

    private final Expression haystack, needle, caseSensitive;

    public StringContains(Source source, Expression haystack, Expression needle, Expression caseSensitive) {
        super(source, Arrays.asList(haystack, needle, toDefault(caseSensitive)));
        this.haystack = haystack;
        this.needle = needle;
        this.caseSensitive = arguments().get(2);
    }

    private static Expression toDefault(Expression exp) {
        return exp != null ? exp : Literal.FALSE;
    }

    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isStringAndExact(haystack, sourceText(), Expressions.ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isStringAndExact(needle, sourceText(), Expressions.ParamOrdinal.SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        return isBoolean(caseSensitive, sourceText(), Expressions.ParamOrdinal.THIRD);
    }

    @Override
    protected Pipe makePipe() {
        return new StringContainsFunctionPipe(source(), this,
                Expressions.pipe(haystack), Expressions.pipe(needle), Expressions.pipe(caseSensitive));
    }

    @Override
    public boolean foldable() {
        return haystack.foldable() && needle.foldable() && caseSensitive.foldable();
    }

    @Override
    public Object fold() {
        return doProcess(haystack.fold(), needle.fold(), caseSensitive.fold());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StringContains::new, haystack, needle, caseSensitive);
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate haystackScript = asScript(haystack);
        ScriptTemplate needleScript = asScript(needle);
        ScriptTemplate caseSensitiveScript = asScript(caseSensitive);

        return asScriptFrom(haystackScript, needleScript, caseSensitiveScript);
    }

    protected ScriptTemplate asScriptFrom(ScriptTemplate haystackScript, ScriptTemplate needleScript, ScriptTemplate caseSensitiveScript) {
        return new ScriptTemplate(format(Locale.ROOT, formatTemplate("{eql}.%s(%s,%s,%s)"),
                "stringContains",
                haystackScript.template(),
                needleScript.template(),
                caseSensitiveScript.template()),
                paramsBuilder()
                        .script(haystackScript.params())
                        .script(needleScript.params())
                        .script(caseSensitiveScript.params())
                        .build(), dataType());
    }

    @Override
    public ScriptTemplate scriptWithField(FieldAttribute field) {
        return new ScriptTemplate(processScript(Scripts.DOC_VALUE),
                paramsBuilder().variable(field.exactAttribute().name()).build(),
                dataType());
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 3) {
            throw new IllegalArgumentException("expected [3] children but received [" + newChildren.size() + "]");
        }

        return new StringContains(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }
}
