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
import static org.elasticsearch.xpack.sql.expression.function.scalar.string.InsertFunctionProcessor.doProcess;
import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * Returns a character string where length characters have been deleted from the source string, beginning at start,
 * and where the replacement string has been inserted into the source string, beginning at start.
 */
public class Insert extends ScalarFunction {

    private final Expression source, start, length, replacement;
    
    public Insert(Location location, Expression source, Expression start, Expression length, Expression replacement) {
        super(location, Arrays.asList(source, start, length, replacement));
        this.source = source;
        this.start = start;
        this.length = length;
        this.replacement = replacement;
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
        
        TypeResolution lengthResolution = Expressions.typeMustBeNumeric(length, functionName(), ParamOrdinal.THIRD);
        if (lengthResolution.unresolved()) {
            return lengthResolution;
        }
        
        return Expressions.typeMustBeString(replacement, functionName(), ParamOrdinal.FOURTH);
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
        return new InsertFunctionPipe(location(), this,
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
                paramsBuilder().variable(field.isInexact() ? field.exactAttribute().name() : field.name()).build(),
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

        return new Insert(location(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }
}
