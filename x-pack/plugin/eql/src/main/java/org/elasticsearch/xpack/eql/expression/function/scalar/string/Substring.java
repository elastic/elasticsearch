/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Expressions.ParamOrdinal;
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
import static org.elasticsearch.xpack.eql.expression.function.scalar.string.SubstringFunctionProcessor.doProcess;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isInteger;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * EQL specific substring function - similar to the one in Python.
 * Note this is different than the one in SQL.
 */
public class Substring extends ScalarFunction implements OptionalArgument {

    private final Expression input, start, end;

    public Substring(Source source, Expression input, Expression start, Expression end) {
        super(source, Arrays.asList(input, start, end != null ? end : new Literal(source, null, DataTypes.NULL)));
        this.input = input;
        this.start = start;
        this.end = arguments().get(2);
    }

    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution sourceResolution = isStringAndExact(input, sourceText(), ParamOrdinal.FIRST);
        if (sourceResolution.unresolved()) {
            return sourceResolution;
        }

        TypeResolution startResolution = isInteger(start, sourceText(), ParamOrdinal.SECOND);
        if (startResolution.unresolved()) {
            return startResolution;
        }

        return isInteger(end, sourceText(), ParamOrdinal.THIRD);
    }

    @Override
    protected Pipe makePipe() {
        return new SubstringFunctionPipe(source(), this, Expressions.pipe(input), Expressions.pipe(start), Expressions.pipe(end));
    }

    @Override
    public boolean foldable() {
        return input.foldable() && start.foldable() && end.foldable();
    }

    @Override
    public Object fold() {
        return doProcess(input.fold(), start.fold(), end.fold());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Substring::new, input, start, end);
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate inputScript = asScript(input);
        ScriptTemplate startScript = asScript(start);
        ScriptTemplate endScript = asScript(end);

        return asScriptFrom(inputScript, startScript, endScript);
    }

    protected ScriptTemplate asScriptFrom(ScriptTemplate inputScript, ScriptTemplate startScript, ScriptTemplate endScript) {
        return new ScriptTemplate(format(Locale.ROOT, formatTemplate("{eql}.%s(%s,%s,%s)"),
                "substring",
                inputScript.template(),
                startScript.template(),
                endScript.template()),
                paramsBuilder()
                    .script(inputScript.params())
                    .script(startScript.params())
                    .script(endScript.params())
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
        return DataTypes.KEYWORD;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 3) {
            throw new IllegalArgumentException("expected [3] children but received [" + newChildren.size() + "]");
        }

        return new Substring(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }
}
