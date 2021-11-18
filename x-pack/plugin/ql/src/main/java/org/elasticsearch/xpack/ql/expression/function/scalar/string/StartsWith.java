/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Arrays;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.function.scalar.string.StartsWithFunctionProcessor.doProcess;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * Function that checks if first parameter starts with the second parameter. Both parameters should be strings
 * and the function returns a boolean value.
 */
public abstract class StartsWith extends CaseInsensitiveScalarFunction {

    private final Expression input;
    private final Expression pattern;

    public StartsWith(Source source, Expression input, Expression pattern, boolean caseInsensitive) {
        super(source, Arrays.asList(input, pattern), caseInsensitive);
        this.input = input;
        this.pattern = pattern;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution fieldResolution = isStringAndExact(input, sourceText(), FIRST);
        if (fieldResolution.unresolved()) {
            return fieldResolution;
        }

        return isStringAndExact(pattern, sourceText(), SECOND);
    }

    public Expression input() {
        return input;
    }

    public Expression pattern() {
        return pattern;
    }

    @Override
    public Pipe makePipe() {
        return new StartsWithFunctionPipe(source(), this, Expressions.pipe(input), Expressions.pipe(pattern), isCaseInsensitive());
    }

    @Override
    public boolean foldable() {
        return input.foldable() && pattern.foldable();
    }

    @Override
    public Object fold() {
        return doProcess(input.fold(), pattern.fold(), isCaseInsensitive());
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate fieldScript = asScript(input);
        ScriptTemplate patternScript = asScript(pattern);

        return asScriptFrom(fieldScript, patternScript);
    }

    protected ScriptTemplate asScriptFrom(ScriptTemplate fieldScript, ScriptTemplate patternScript) {
        ParamsBuilder params = paramsBuilder();

        String template = formatTemplate("{ql}.startsWith(" + fieldScript.template() + ", " + patternScript.template() + ", {})");
        params.script(fieldScript.params()).script(patternScript.params()).variable(isCaseInsensitive());

        return new ScriptTemplate(template, params.build(), dataType());
    }

    @Override
    public ScriptTemplate scriptWithField(FieldAttribute field) {
        return new ScriptTemplate(
            processScript(Scripts.DOC_VALUE),
            paramsBuilder().variable(field.exactAttribute().name()).build(),
            dataType()
        );
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }
}
