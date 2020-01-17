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
import org.elasticsearch.xpack.ql.expression.function.OptionalArgument;
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
import static org.elasticsearch.xpack.sql.expression.function.scalar.string.LocateFunctionProcessor.doProcess;

/**
 * Returns the starting position of the first occurrence of the pattern within the source string.
 * The search for the first occurrence of the pattern begins with the first character position in the source string
 * unless the optional argument, start, is specified. If start is specified, the search begins with the character
 * position indicated by the value of start. The first character position in the source string is indicated by the value 1.
 * If the pattern is not found within the source string, the value 0 is returned.
 */
public class Locate extends ScalarFunction implements OptionalArgument {

    private final Expression pattern, source, start;
    
    public Locate(Source source, Expression pattern, Expression src, Expression start) {
        super(source, start != null ? Arrays.asList(pattern, src, start) : Arrays.asList(pattern, src));
        this.pattern = pattern;
        this.source = src;
        this.start = start;
    }
    
    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution patternResolution = isStringAndExact(pattern, sourceText(), ParamOrdinal.FIRST);
        if (patternResolution.unresolved()) {
            return patternResolution;
        }
        
        TypeResolution sourceResolution = isStringAndExact(source, sourceText(), ParamOrdinal.SECOND);
        if (sourceResolution.unresolved()) {
            return sourceResolution;
        }

        return start == null ? TypeResolution.TYPE_RESOLVED : isNumeric(start, sourceText(), ParamOrdinal.THIRD);
    }

    @Override
    protected Pipe makePipe() {
        return new LocateFunctionPipe(source(), this,
            Expressions.pipe(pattern),
            Expressions.pipe(source),
            start == null ? null : Expressions.pipe(start));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Locate::new, pattern, source, start);
    }

    @Override
    public boolean foldable() {
        return pattern.foldable()
                && source.foldable()
                && (start == null || start.foldable());
    }

    @Override
    public Object fold() {
        return doProcess(pattern.fold(), source.fold(), (start == null ? null : start.fold()));
    }
    
    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate patternScript = asScript(pattern);
        ScriptTemplate sourceScript = asScript(source);
        ScriptTemplate startScript = start == null ? null : asScript(start);

        return asScriptFrom(patternScript, sourceScript, startScript);
    }

    private ScriptTemplate asScriptFrom(ScriptTemplate patternScript, ScriptTemplate sourceScript, ScriptTemplate startScript) {
        if (start == null) {
            return new ScriptTemplate(format(Locale.ROOT, formatTemplate("{sql}.%s(%s,%s)"),
                    "locate",
                    patternScript.template(),
                    sourceScript.template()),
                    paramsBuilder()
                        .script(patternScript.params()).script(sourceScript.params())
                        .build(), dataType());
        }
        // basically, transform the script to InternalSqlScriptUtils.[function_name](function_or_field1, function_or_field2,...)
        return new ScriptTemplate(format(Locale.ROOT, formatTemplate("{sql}.%s(%s,%s,%s)"),
                "locate",
                patternScript.template(),
                sourceScript.template(),
                startScript.template()),
                paramsBuilder()
                    .script(patternScript.params()).script(sourceScript.params())
                    .script(startScript.params())
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
        return DataType.INTEGER;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (start != null && newChildren.size() != 3) {
            throw new IllegalArgumentException("expected [3] children but received [" + newChildren.size() + "]");
        } else if (start == null && newChildren.size() != 2) {
            throw new IllegalArgumentException("expected [2] children but received [" + newChildren.size() + "]");
        }

        return new Locate(source(), newChildren.get(0), newChildren.get(1), start == null ? null : newChildren.get(2));
    }
}
