/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.common.logging.LoggerMessageFormat;
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
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.eql.expression.function.scalar.string.CIDRMatchFunctionProcessor.doProcess;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.fromIndex;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isIPAndExact;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * EQL specific cidrMatch function
 * Returns true if the source address matches any of the provided CIDR blocks.
 * Refer to: https://eql.readthedocs.io/en/latest/query-guide/functions.html#cidrMatch
 */
public class CIDRMatch extends ScalarFunction {

    private final Expression input;
    private final List<Expression> addresses;

    public CIDRMatch(Source source, Expression input, List<Expression> addresses) {
        super(source, CollectionUtils.combine(singletonList(input), addresses == null ? emptyList() : addresses));
        this.input = input;
        this.addresses = addresses == null ? emptyList() : addresses;
    }

    public Expression input() {
        return input;
    }

    public List<Expression> addresses() {
        return addresses;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isIPAndExact(input, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        int index = 1;
        for (Expression addr : addresses) {
            resolution = isFoldable(addr, sourceText(), fromIndex(index));
            if (resolution.unresolved()) {
                break;
            }

            resolution = isStringAndExact(addr, sourceText(), fromIndex(index));
            if (resolution.unresolved()) {
                break;
            }

            index++;
        }

        return resolution;
    }

    @Override
    protected Pipe makePipe() {
        return new CIDRMatchFunctionPipe(source(), this, Expressions.pipe(input), Expressions.pipe(addresses));
    }

    @Override
    public boolean foldable() {
        return input.foldable() && Expressions.foldable(addresses);
    }

    @Override
    public Object fold() {
        return doProcess(input.fold(), Expressions.fold(addresses));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, CIDRMatch::new, input, addresses);
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate leftScript = asScript(input);

        List<Object> values = new ArrayList<>(new LinkedHashSet<>(Expressions.fold(addresses)));
        return new ScriptTemplate(
                formatTemplate(LoggerMessageFormat.format("{eql}.","cidrMatch({}, {})", leftScript.template())),
                paramsBuilder()
                        .script(leftScript.params())
                        .variable(values)
                        .build(),
                dataType());
    }

    @Override
    public ScriptTemplate scriptWithField(FieldAttribute field) {
        return new ScriptTemplate(processScript(Scripts.DOC_VALUE),
                paramsBuilder().variable(field.exactAttribute().name()).build(),
                dataType());
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new CIDRMatch(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }
}
