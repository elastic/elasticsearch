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
import static org.elasticsearch.xpack.eql.expression.function.scalar.string.IndexOfFunctionProcessor.doProcess;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isInteger;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * Find the first position (zero-indexed) of a string where a substring is found.
 * If the optional parameter start is provided, then this will find the first occurrence at or after the start position.
 */
public class IndexOf extends ScalarFunction implements OptionalArgument {

    private final Expression source, substring, start;

    public IndexOf(Source source, Expression src, Expression substring, Expression start) {
        super(source, Arrays.asList(src, substring, start != null ? start : new Literal(source, null, DataTypes.NULL)));
        this.source = src;
        this.substring = substring;
        this.start = arguments().get(2);
    }

    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isStringAndExact(source, sourceText(), ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isStringAndExact(substring, sourceText(), ParamOrdinal.SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        
        return isInteger(start, sourceText(), ParamOrdinal.THIRD);
    }

    @Override
    protected Pipe makePipe() {
        return new IndexOfFunctionPipe(source(), this, Expressions.pipe(source), Expressions.pipe(substring), Expressions.pipe(start));
    }

    @Override
    public boolean foldable() {
        return source.foldable() && substring.foldable() && start.foldable();
    }

    @Override
    public Object fold() {
        return doProcess(source.fold(), substring.fold(), start.fold());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, IndexOf::new, source, substring, start);
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate sourceScript = asScript(source);
        ScriptTemplate substringScript = asScript(substring);
        ScriptTemplate startScript = asScript(start);

        return asScriptFrom(sourceScript, substringScript, startScript);
    }
    
    protected ScriptTemplate asScriptFrom(ScriptTemplate sourceScript, ScriptTemplate substringScript, ScriptTemplate startScript) {
        return new ScriptTemplate(format(Locale.ROOT, formatTemplate("{eql}.%s(%s,%s,%s)"),
                "indexOf",
                sourceScript.template(),
                substringScript.template(),
                startScript.template()),
                paramsBuilder()
                    .script(sourceScript.params())
                    .script(substringScript.params())
                    .script(startScript.params())
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
        return DataTypes.INTEGER;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 3) {
            throw new IllegalArgumentException("expected [3] children but received [" + newChildren.size() + "]");
        }

        return new IndexOf(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

}