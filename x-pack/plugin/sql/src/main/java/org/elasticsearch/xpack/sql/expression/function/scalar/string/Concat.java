/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinitions;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Locale;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate.formatTemplate;
import static org.elasticsearch.xpack.sql.expression.function.scalar.string.ConcatFunctionProcessor.doProcessInScripts;

/**
 * Returns a string that is the result of concatenating the two strings received as parameters.
 * The result of the function is null only if both parameters are null, otherwise the result is the non-null
 * parameter or the concatenation of the two strings if none of them is null.
 */
public class Concat extends BinaryScalarFunction {
    
    public Concat(Location location, Expression source1, Expression source2) {
        super(location, source1, source2);
    }

    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution sourceResolution = StringFunctionUtils.resolveStringInputType(left().dataType(), functionName());
        if (sourceResolution != TypeResolution.TYPE_RESOLVED) {
            return sourceResolution;
        }

        return StringFunctionUtils.resolveStringInputType(right().dataType(), functionName());
    }

    @Override
    protected ProcessorDefinition makeProcessorDefinition() {
        return new ConcatFunctionProcessorDefinition(location(), this,
                ProcessorDefinitions.toProcessorDefinition(left()),
                ProcessorDefinitions.toProcessorDefinition(right()));
    }
    
    @Override
    public boolean foldable() {
        return left().foldable() 
                && right().foldable();
    }

    @Override
    public Object fold() {
        return doProcessInScripts(left().fold(), right().fold());
    }
    
    @Override
    protected Concat replaceChildren(Expression newLeft, Expression newRight) {
        return new Concat(location(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Concat::new, left(), right());
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate sourceScript1 = asScript(left());
        ScriptTemplate sourceScript2 = asScript(right());

        return asScriptFrom(sourceScript1, sourceScript2);
    }

    @Override
    protected ScriptTemplate asScriptFrom(ScriptTemplate leftScript, ScriptTemplate rightScript) {
        // basically, transform the script to InternalSqlScriptUtils.[function_name](function_or_field1, function_or_field2)
        return new ScriptTemplate(format(Locale.ROOT, formatTemplate("{sql}.%s(%s,%s)"), 
                "concat", 
                leftScript.template(), 
                rightScript.template()),
                paramsBuilder()
                    .script(leftScript.params()).script(rightScript.params())
                    .build(), dataType());
    }
    
    @Override
    protected ScriptTemplate asScriptFrom(FieldAttribute field) {
        return new ScriptTemplate(formatScript("doc[{}].value"),
                paramsBuilder().variable(field.isInexact() ? field.exactAttribute().name() : field.name()).build(),
                dataType());
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

}
