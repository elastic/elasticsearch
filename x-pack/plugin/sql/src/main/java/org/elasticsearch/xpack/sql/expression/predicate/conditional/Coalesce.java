/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypeConversion;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

public class Coalesce extends ConditionalFunction {

    private DataType dataType = DataType.NULL;

    public Coalesce(Location location, List<Expression> fields) {
        super(location, fields);
    }

    @Override
    protected NodeInfo<Coalesce> info() {
        return NodeInfo.create(this, Coalesce::new, children());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Coalesce(location(), newChildren);
    }

    @Override
    protected TypeResolution resolveType() {
        for (Expression e : children()) {
            dataType = DataTypeConversion.commonType(dataType, e.dataType());
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean foldable() {
        // if the first entry is foldable, so is coalesce
        // that's because the nulls are eliminated by the optimizer
        // and if the first expression is folded (and not null), the rest do not matter
        List<Expression> children = children();
        return (children.isEmpty() || (children.get(0).foldable() && children.get(0).fold() != null));
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public Object fold() {
        List<Expression> children = children();
        return children.isEmpty() ? null : children.get(0).fold();
    }

    @Override
    public ScriptTemplate asScript() {
        List<ScriptTemplate> templates = new ArrayList<>();
        for (Expression ex : children()) {
            templates.add(asScript(ex));
        }

        StringJoiner template = new StringJoiner(",", "{sql}.coalesce([", "])");
        ParamsBuilder params = paramsBuilder();

        for (ScriptTemplate scriptTemplate : templates) {
            template.add(scriptTemplate.template());
            params.script(scriptTemplate.params());
        }

        return new ScriptTemplate(template.toString(), params.build(), dataType);
    }

    @Override
    protected Pipe makePipe() {
        return new CoalescePipe(location(), this, Expressions.pipe(children()));
    }
}
