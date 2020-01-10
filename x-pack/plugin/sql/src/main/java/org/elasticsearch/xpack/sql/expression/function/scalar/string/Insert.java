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
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.string.InsertFunctionProcessor.doProcess;

/**
 * Returns a character string where length characters have been deleted from the source string, beginning at start,
 * and where the replacement string has been inserted into the source string, beginning at start.
 */
public class Insert extends ScalarFunction {

    private final Expression source, start, length, replacement;
    
    public Insert(Source source, Expression src, Expression start, Expression length, Expression replacement) {
        super(source, Arrays.asList(src, start, length, replacement));
        this.source = src;
        this.start = start;
        this.length = length;
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

        TypeResolution startResolution = isNumeric(start, sourceText(), ParamOrdinal.SECOND);
        if (startResolution.unresolved()) {
            return startResolution;
        }
        
        TypeResolution lengthResolution = isNumeric(length, sourceText(), ParamOrdinal.THIRD);
        if (lengthResolution.unresolved()) {
            return lengthResolution;
        }
        
        return isStringAndExact(replacement, sourceText(), ParamOrdinal.FOURTH);
    }

    @Override
    public boolean foldable() {
        return source.foldable()
                && start.foldable()
                && length.foldable()
                && replacement.foldable();
    }

    @Override
    public Object fold() {
        return doProcess(source.fold(), start.fold(), length.fold(), replacement.fold());
    }

    @Override
    protected Pipe makePipe() {
        return new InsertFunctionPipe(source(), this,
                Expressions.pipe(source),
                Expressions.pipe(start),
                Expressions.pipe(length),
                Expressions.pipe(replacement));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Insert::new, source, start, length, replacement);
    }
    
    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate sourceScript = asScript(source);
        ScriptTemplate startScript = asScript(start);
        ScriptTemplate lengthScript = asScript(length);
        ScriptTemplate replacementScript = asScript(replacement);

        return asScriptFrom(sourceScript, startScript, lengthScript, replacementScript);
    }

    private ScriptTemplate asScriptFrom(ScriptTemplate sourceScript, ScriptTemplate startScript,
            ScriptTemplate lengthScript, ScriptTemplate replacementScript) {
        // basically, transform the script to InternalSqlScriptUtils.[function_name](function_or_field1, function_or_field2,...)
        return new ScriptTemplate(format(Locale.ROOT, formatTemplate("{sql}.%s(%s,%s,%s,%s)"),
                "insert",
                sourceScript.template(),
                startScript.template(),
                lengthScript.template(),
                replacementScript.template()),
                paramsBuilder()
                    .script(sourceScript.params()).script(startScript.params())
                    .script(lengthScript.params()).script(replacementScript.params())
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
        if (newChildren.size() != 4) {
            throw new IllegalArgumentException("expected [4] children but received [" + newChildren.size() + "]");
        }

        return new Insert(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }
}
