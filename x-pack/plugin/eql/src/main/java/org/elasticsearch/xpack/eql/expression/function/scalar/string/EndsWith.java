/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.function.scalar.string.CaseSensitiveScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static java.lang.String.format;
import static org.elasticsearch.xpack.eql.expression.function.scalar.string.EndsWithFunctionProcessor.doProcess;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * Function that checks if first parameter ends with the second parameter. Both parameters should be strings
 * and the function returns a boolean value. The function is case insensitive.
 */
public class EndsWith extends CaseSensitiveScalarFunction {

    private final Expression source;
    private final Expression pattern;

    public EndsWith(Source source, Expression src, Expression pattern, Configuration configuration) {
        super(source, Arrays.asList(src, pattern), configuration);
        this.source = src;
        this.pattern = pattern;
    }

    @Override
    public boolean isCaseSensitive() {
        return ((EqlConfiguration) configuration()).isCaseSensitive();
    }

    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution sourceResolution = isStringAndExact(source, sourceText(), ParamOrdinal.FIRST);
        if (sourceResolution.unresolved()) {
            return sourceResolution;
        }

        return isStringAndExact(pattern, sourceText(), ParamOrdinal.SECOND);
    }

    @Override
    protected Pipe makePipe() {
        return new EndsWithFunctionPipe(source(), this, Expressions.pipe(source), Expressions.pipe(pattern), isCaseSensitive());
    }

    @Override
    public boolean foldable() {
        return source.foldable() && pattern.foldable();
    }

    @Override
    public Object fold() {
        return doProcess(source.fold(), pattern.fold(), isCaseSensitive());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, EndsWith::new, source, pattern, configuration());
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate sourceScript = asScript(source);
        ScriptTemplate patternScript = asScript(pattern);

        return asScriptFrom(sourceScript, patternScript);
    }
    
    protected ScriptTemplate asScriptFrom(ScriptTemplate sourceScript, ScriptTemplate patternScript) {
        return new ScriptTemplate(format(Locale.ROOT, formatTemplate("{eql}.%s(%s,%s,%s)"),
                "endsWith",
                sourceScript.template(),
                patternScript.template(),
                "{}"),
                paramsBuilder()
                        .script(sourceScript.params())
                        .script(patternScript.params())
                        .variable(isCaseSensitive())
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
        if (newChildren.size() != 2) {
            throw new IllegalArgumentException("expected [2] children but received [" + newChildren.size() + "]");
        }

        return new EndsWith(source(), newChildren.get(0), newChildren.get(1), configuration());
    }

}
