/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * Implements the CASE WHEN ... THEN ... ELSE ... END expression
 */
public class Case extends ConditionalFunction {

    private final List<IfConditional> conditions;
    private final Expression elseResult;

    @SuppressWarnings("unchecked")
    public Case(Source source, List<Expression> expressions) {
        super(source, expressions);
        this.conditions = (List<IfConditional>) (List<?>) expressions.subList(0, expressions.size() - 1);
        this.elseResult = expressions.get(expressions.size() - 1);
    }

    public List<IfConditional> conditions() {
        return conditions;
    }

    public Expression elseResult() {
        return elseResult;
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            if (conditions.isEmpty()) {
                dataType = elseResult().dataType();
            } else {
                dataType = DataTypes.NULL;

                for (IfConditional conditional : conditions) {
                    dataType = SqlDataTypeConverter.commonType(dataType, conditional.dataType());
                }
                dataType = SqlDataTypeConverter.commonType(dataType, elseResult.dataType());
            }
        }
        return dataType;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Case(source(), newChildren);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Case::new, children());
    }

    @Override
    protected TypeResolution resolveType() {
        DataType expectedResultDataType = null;
        for (IfConditional ifConditional : conditions) {
            if (DataTypes.isNull(ifConditional.result().dataType()) == false) {
                expectedResultDataType = ifConditional.result().dataType();
                break;
            }
        }
        if (expectedResultDataType == null) {
            expectedResultDataType = elseResult().dataType();
        }

        for (IfConditional conditional : conditions) {
            if (conditional.condition().dataType() != DataTypes.BOOLEAN) {
                return new TypeResolution(format(null, "condition of [{}] must be [boolean], found value [{}] type [{}]",
                    conditional.sourceText(),
                    Expressions.name(conditional.condition()),
                    conditional.condition().dataType().typeName()));
            }
            if (SqlDataTypes.areCompatible(expectedResultDataType, conditional.dataType()) == false) {
                return new TypeResolution(format(null, "result of [{}] must be [{}], found value [{}] type [{}]",
                    conditional.sourceText(),
                    expectedResultDataType.typeName(),
                    Expressions.name(conditional.result()),
                    conditional.dataType().typeName()));
            }
        }

        if (SqlDataTypes.areCompatible(expectedResultDataType, elseResult.dataType()) == false) {
            return new TypeResolution(format(null, "ELSE clause of [{}] must be [{}], found value [{}] type [{}]",
                elseResult.sourceText(),
                expectedResultDataType.typeName(),
                Expressions.name(elseResult),
                elseResult.dataType().typeName()));
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    /**
     * All foldable conditions that fold to FALSE should have
     * been removed by the Optimizer.
     */
    @Override
    public boolean foldable() {
        if (conditions.isEmpty() && elseResult.foldable()) {
            return true;
        }
        if (conditions.size() == 1 && conditions.get(0).condition().foldable()) {
            if (conditions.get(0).condition().fold() == Boolean.TRUE) {
                return conditions().get(0).result().foldable();
            } else {
                return elseResult().foldable();
            }
        }
        return false;
    }

    @Override
    public Object fold() {
        if (conditions.isEmpty() == false && conditions.get(0).condition().fold() == Boolean.TRUE) {
            return conditions.get(0).result().fold();
        }
        return elseResult.fold();
    }

    @Override
    protected Pipe makePipe() {
        List<Pipe> pipes = new ArrayList<>(conditions.size() + 1);
        for (IfConditional ifConditional : conditions) {
            pipes.add(Expressions.pipe(ifConditional.condition()));
            pipes.add(Expressions.pipe(ifConditional.result()));
        }
        pipes.add(Expressions.pipe(elseResult));
        return new CasePipe(source(), this, pipes);
    }

    @Override
    public ScriptTemplate asScript() {
        List<ScriptTemplate> templates = new ArrayList<>();
        for (IfConditional ifConditional : conditions) {
            templates.add(asScript(ifConditional.condition()));
            templates.add(asScript(ifConditional.result()));
        }
        templates.add(asScript(elseResult));

        // Use painless ?: expressions to prevent evaluation of return expression
        // if the condition which guards it evaluates to false (e.g. division by 0)
        StringBuilder sb = new StringBuilder();
        ParamsBuilder params = paramsBuilder();
        for (int i = 0; i < templates.size(); i++) {
            ScriptTemplate scriptTemplate = templates.get(i);
            if (i < templates.size() - 1) {
                if (i % 2 == 0) {
                    // painless ? : operator expects primitive boolean, thus we use nullSafeFilter
                    // to convert object Boolean to primitive boolean (null => false)
                    sb.append(Scripts.nullSafeFilter(scriptTemplate).template()).append(" ? ");
                } else {
                    sb.append(scriptTemplate.template()).append(" : ");
                }
            } else {
                sb.append(scriptTemplate.template());
            }
            params.script(scriptTemplate.params());
        }

        return new ScriptTemplate(formatTemplate(sb.toString()), params.build(), dataType());
    }
}
