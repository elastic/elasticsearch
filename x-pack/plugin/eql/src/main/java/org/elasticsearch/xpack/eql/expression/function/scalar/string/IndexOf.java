/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.ql.expression.function.scalar.string.CaseInsensitiveScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.Locale;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.eql.expression.function.scalar.string.IndexOfFunctionProcessor.doProcess;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isInteger;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * Find the first position (zero-indexed) of a string where a substring is found.
 * If the optional parameter start is provided, then this will find the first occurrence at or after the start position.
 */
public class IndexOf extends CaseInsensitiveScalarFunction implements OptionalArgument {

    private final Expression input, substring, start;

    public IndexOf(Source source, Expression input, Expression substring, Expression start, boolean caseInsensitive) {
        super(source, asList(input, substring, start != null ? start : new Literal(source, null, DataTypes.NULL)), caseInsensitive);
        this.input = input;
        this.substring = substring;
        this.start = arguments().get(2);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isStringAndExact(input, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isStringAndExact(substring, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isInteger(start, sourceText(), THIRD);
    }

    @Override
    protected Pipe makePipe() {
        return new IndexOfFunctionPipe(
            source(),
            this,
            Expressions.pipe(input),
            Expressions.pipe(substring),
            Expressions.pipe(start),
            isCaseInsensitive()
        );
    }

    @Override
    public boolean foldable() {
        return input.foldable() && substring.foldable() && start.foldable();
    }

    @Override
    public Object fold() {
        return doProcess(input.fold(), substring.fold(), start.fold(), isCaseInsensitive());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, IndexOf::new, input, substring, start, isCaseInsensitive());
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate inputScript = asScript(input);
        ScriptTemplate substringScript = asScript(substring);
        ScriptTemplate startScript = asScript(start);

        return asScriptFrom(inputScript, substringScript, startScript);
    }

    protected ScriptTemplate asScriptFrom(ScriptTemplate inputScript, ScriptTemplate substringScript, ScriptTemplate startScript) {
        return new ScriptTemplate(
            format(
                Locale.ROOT,
                formatTemplate("{eql}.%s(%s,%s,%s,%s)"),
                "indexOf",
                inputScript.template(),
                substringScript.template(),
                startScript.template(),
                "{}"
            ),
            paramsBuilder().script(inputScript.params())
                .script(substringScript.params())
                .script(startScript.params())
                .variable(isCaseInsensitive())
                .build(),
            dataType()
        );
    }

    @Override
    public DataType dataType() {
        return DataTypes.INTEGER;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new IndexOf(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), isCaseInsensitive());
    }
}
