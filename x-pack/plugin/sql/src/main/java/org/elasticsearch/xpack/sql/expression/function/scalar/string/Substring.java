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
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isInteger;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.string.SubstringFunctionProcessor.doProcess;

/**
 * Returns a character string that is derived from the source string, beginning at the character position specified by start
 * for length characters.
 */
public class Substring extends ScalarFunction {

    private final Expression input, start, length;

    public Substring(Source source, Expression input, Expression start, Expression length) {
        super(source, Arrays.asList(input, start, length));
        this.input = input;
        this.start = start;
        this.length = length;
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

        TypeResolution startResolution = isInteger(start, sourceText(), SECOND);
        if (startResolution.unresolved()) {
            return startResolution;
        }

        return isInteger(length, sourceText(), THIRD);
    }

    @Override
    protected Pipe makePipe() {
        return new SubstringFunctionPipe(source(), this, Expressions.pipe(input), Expressions.pipe(start), Expressions.pipe(length));
    }

    @Override
    public boolean foldable() {
        return input.foldable() && start.foldable() && length.foldable();
    }

    @Override
    public Object fold() {
        return doProcess(input.fold(), start.fold(), length.fold());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Substring::new, input, start, length);
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate inputScript = asScript(input);
        ScriptTemplate startScript = asScript(start);
        ScriptTemplate lengthScript = asScript(length);

        return asScriptFrom(inputScript, startScript, lengthScript);
    }

    protected ScriptTemplate asScriptFrom(ScriptTemplate inputScript, ScriptTemplate startScript,
            ScriptTemplate lengthScript) {
        // basically, transform the script to InternalSqlScriptUtils.[function_name](function_or_field1, function_or_field2,...)
        return new ScriptTemplate(format(Locale.ROOT, formatTemplate("{sql}.%s(%s,%s,%s)"),
                "substring",
                inputScript.template(),
                startScript.template(),
                lengthScript.template()),
                paramsBuilder()
                    .script(inputScript.params()).script(startScript.params())
                    .script(lengthScript.params())
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
        return DataTypes.KEYWORD;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Substring(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }
}
