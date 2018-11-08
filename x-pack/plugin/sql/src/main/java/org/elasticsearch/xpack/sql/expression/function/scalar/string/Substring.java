/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.expression.function.scalar.string.SubstringFunctionProcessor.doProcess;
import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * Returns a character string that is derived from the source string, beginning at the character position specified by start
 * for length characters.
 */
public class Substring extends ScalarFunction {

    private final Expression source, start, length;

    public Substring(Location location, Expression source, Expression start, Expression length) {
        super(location, Arrays.asList(source, start, length));
        this.source = source;
        this.start = start;
        this.length = length;
    }

    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution sourceResolution = Expressions.typeMustBeString(source, functionName(), ParamOrdinal.FIRST);
        if (sourceResolution.unresolved()) {
            return sourceResolution;
        }

        TypeResolution startResolution = Expressions.typeMustBeNumeric(start, functionName(), ParamOrdinal.SECOND);
        if (startResolution.unresolved()) {
            return startResolution;
        }

        return Expressions.typeMustBeNumeric(length, functionName(), ParamOrdinal.THIRD);
    }

    @Override
    protected Pipe makePipe() {
        return new SubstringFunctionPipe(location(), this,
                Expressions.pipe(source),
                Expressions.pipe(start),
                Expressions.pipe(length));
    }
    
    @Override
    public boolean foldable() {
        return source.foldable() && start.foldable() && length.foldable();
    }

    @Override
    public Object fold() {
        return doProcess(source.fold(), start.fold(), length.fold());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Substring::new, source, start, length);
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate sourceScript = asScript(source);
        ScriptTemplate startScript = asScript(start);
        ScriptTemplate lengthScript = asScript(length);

        return asScriptFrom(sourceScript, startScript, lengthScript);
    }

    protected ScriptTemplate asScriptFrom(ScriptTemplate sourceScript, ScriptTemplate startScript,
            ScriptTemplate lengthScript) {
        // basically, transform the script to InternalSqlScriptUtils.[function_name](function_or_field1, function_or_field2,...)
        return new ScriptTemplate(format(Locale.ROOT, formatTemplate("{sql}.%s(%s,%s,%s)"),
                "substring",
                sourceScript.template(),
                startScript.template(),
                lengthScript.template()),
                paramsBuilder()
                    .script(sourceScript.params()).script(startScript.params())
                    .script(lengthScript.params())
                    .build(), dataType());
    }
    
    @Override
    public ScriptTemplate scriptWithField(FieldAttribute field) {
        return new ScriptTemplate(processScript("doc[{}].value"),
                paramsBuilder().variable(field.isInexact() ? field.exactAttribute().name() : field.name()).build(),
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

        return new Substring(location(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }
}
