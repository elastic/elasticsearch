/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static java.lang.String.format;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.string.ReplaceFunctionProcessor.doProcess;

/**
 * Search the source string for occurrences of the pattern, and replace with the replacement string.
 */
public class Replace extends ScalarFunction {

    private final Expression source, pattern, replacement;

    public Replace(Source source, Expression src, Expression pattern, Expression replacement) {
        super(source, Arrays.asList(src, pattern, replacement));
        this.source = src;
        this.pattern = pattern;
        this.replacement = replacement;
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

        TypeResolution patternResolution = isStringAndExact(pattern, sourceText(), ParamOrdinal.SECOND);
        if (patternResolution.unresolved()) {
            return patternResolution;
        }

        return isStringAndExact(replacement, sourceText(), ParamOrdinal.THIRD);
    }

    @Override
    protected Pipe makePipe() {
        return new ReplaceFunctionPipe(source(), this,
                Expressions.pipe(source),
                Expressions.pipe(pattern),
                Expressions.pipe(replacement));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Replace::new, source, pattern, replacement);
    }

    @Override
    public boolean foldable() {
        return source.foldable()
                && pattern.foldable()
                && replacement.foldable();
    }

    @Override
    public Object fold() {
        return doProcess(source.fold(), pattern.fold(), replacement.fold());
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate sourceScript = asScript(source);
        ScriptTemplate patternScript = asScript(pattern);
        ScriptTemplate replacementScript = asScript(replacement);

        return asScriptFrom(sourceScript, patternScript, replacementScript);
    }

    private ScriptTemplate asScriptFrom(ScriptTemplate sourceScript, ScriptTemplate patternScript, ScriptTemplate replacementScript) {
        // basically, transform the script to InternalSqlScriptUtils.[function_name](function_or_field1, function_or_field2,...)
        return new ScriptTemplate(format(Locale.ROOT, formatTemplate("{sql}.%s(%s,%s,%s)"),
                "replace",
                sourceScript.template(),
                patternScript.template(),
                replacementScript.template()),
                paramsBuilder()
                    .script(sourceScript.params()).script(patternScript.params())
                    .script(replacementScript.params())
                    .build(), dataType());
    }

    @Override
    public ScriptTemplate scriptWithField(FieldAttribute field) {
        return new ScriptTemplate(processScript("doc[{}].value"),
                paramsBuilder().variable(field.exactAttribute().name()).build(),
                dataType());
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 3) {
            throw new IllegalArgumentException("expected [3] children but received [" + newChildren.size() + "]");
        }

        return new Replace(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }
}
