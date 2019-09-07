/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.NodeInfo.NodeCtor2;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.time.ZoneId;

import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

public class Quarter extends BaseDateTimeFunction {

    public Quarter(Source source, Expression field, ZoneId zoneId) {
        super(source, field, zoneId);
    }
    
    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate script = super.asScript();
        String template = formatTemplate("{sql}.quarter(" + script.template() + ", {})");
        
        ParamsBuilder params = paramsBuilder().script(script.params()).variable(zoneId().getId());
        
        return new ScriptTemplate(template, params.build(), dataType());
    }

    @Override
    protected NodeCtor2<Expression, ZoneId, BaseDateTimeFunction> ctorForInfo() {
        return Quarter::new;
    }

    @Override
    protected Quarter replaceChild(Expression newChild) {
        return new Quarter(source(), newChild, zoneId());
    }

    @Override
    protected Processor makeProcessor() {
        return new QuarterProcessor(zoneId());
    }

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }
}
