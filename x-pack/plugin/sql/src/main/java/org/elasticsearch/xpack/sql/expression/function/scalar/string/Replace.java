/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
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
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.string.ReplaceFunctionProcessor.doProcess;

/**
 * Search the source string for occurrences of the pattern, and replace with the replacement string.
 */
public class Replace extends ScalarFunction {

    private final Expression input, pattern, replacement;

    public Replace(Source source, Expression input, Expression pattern, Expression replacement) {
        super(source, Arrays.asList(input, pattern, replacement));
        this.input = input;
        this.pattern = pattern;
        this.replacement = replacement;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution sourceResolution = isStringAndExact(input, sourceText(), FIRST);
        if (sourceResolution.unresolved()) {
            return sourceResolution;
        }

        TypeResolution patternResolution = isStringAndExact(pattern, sourceText(), SECOND);
        if (patternResolution.unresolved()) {
            return patternResolution;
        }

        return isStringAndExact(replacement, sourceText(), THIRD);
    }

    @Override
    protected Pipe makePipe() {
        return new ReplaceFunctionPipe(source(), this,
                Expressions.pipe(input),
                Expressions.pipe(pattern),
                Expressions.pipe(replacement));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Replace::new, input, pattern, replacement);
    }

    @Override
    public boolean foldable() {
        return input.foldable()
                && pattern.foldable()
                && replacement.foldable();
    }

    @Override
    public Object fold() {
        return doProcess(input.fold(), pattern.fold(), replacement.fold());
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate inputScript = asScript(input);
        ScriptTemplate patternScript = asScript(pattern);
        ScriptTemplate replacementScript = asScript(replacement);

        return asScriptFrom(inputScript, patternScript, replacementScript);
    }

    private ScriptTemplate asScriptFrom(ScriptTemplate inputScript, ScriptTemplate patternScript, ScriptTemplate replacementScript) {
        // basically, transform the script to InternalSqlScriptUtils.[function_name](function_or_field1, function_or_field2,...)
        return new ScriptTemplate(format(Locale.ROOT, formatTemplate("{sql}.%s(%s,%s,%s)"),
                "replace",
                inputScript.template(),
                patternScript.template(),
                replacementScript.template()),
                paramsBuilder()
                    .script(inputScript.params()).script(patternScript.params())
                    .script(replacementScript.params())
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
        return new Replace(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }
}
