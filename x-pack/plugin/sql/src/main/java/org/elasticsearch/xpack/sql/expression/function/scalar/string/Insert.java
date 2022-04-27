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
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.string.InsertFunctionProcessor.doProcess;

/**
 * Returns a character string where length characters have been deleted from the source string, beginning at start,
 * and where the replacement string has been inserted into the source string, beginning at start.
 */
public class Insert extends ScalarFunction {

    private final Expression input, start, length, replacement;

    public Insert(Source source, Expression input, Expression start, Expression length, Expression replacement) {
        super(source, Arrays.asList(input, start, length, replacement));
        this.input = input;
        this.start = start;
        this.length = length;
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

        TypeResolution startResolution = isNumeric(start, sourceText(), SECOND);
        if (startResolution.unresolved()) {
            return startResolution;
        }

        TypeResolution lengthResolution = isNumeric(length, sourceText(), THIRD);
        if (lengthResolution.unresolved()) {
            return lengthResolution;
        }

        return isStringAndExact(replacement, sourceText(), FOURTH);
    }

    @Override
    public boolean foldable() {
        return input.foldable() && start.foldable() && length.foldable() && replacement.foldable();
    }

    @Override
    public Object fold() {
        return doProcess(input.fold(), start.fold(), length.fold(), replacement.fold());
    }

    @Override
    protected Pipe makePipe() {
        return new InsertFunctionPipe(
            source(),
            this,
            Expressions.pipe(input),
            Expressions.pipe(start),
            Expressions.pipe(length),
            Expressions.pipe(replacement)
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Insert::new, input, start, length, replacement);
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate inputScript = asScript(input);
        ScriptTemplate startScript = asScript(start);
        ScriptTemplate lengthScript = asScript(length);
        ScriptTemplate replacementScript = asScript(replacement);

        return asScriptFrom(inputScript, startScript, lengthScript, replacementScript);
    }

    private ScriptTemplate asScriptFrom(
        ScriptTemplate inputScript,
        ScriptTemplate startScript,
        ScriptTemplate lengthScript,
        ScriptTemplate replacementScript
    ) {
        // basically, transform the script to InternalSqlScriptUtils.[function_name](function_or_field1, function_or_field2,...)
        return new ScriptTemplate(
            format(
                Locale.ROOT,
                formatTemplate("{sql}.%s(%s,%s,%s,%s)"),
                "insert",
                inputScript.template(),
                startScript.template(),
                lengthScript.template(),
                replacementScript.template()
            ),
            paramsBuilder().script(inputScript.params())
                .script(startScript.params())
                .script(lengthScript.params())
                .script(replacementScript.params())
                .build(),
            dataType()
        );
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
        return DataTypes.KEYWORD;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Insert(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }
}
