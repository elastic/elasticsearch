/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.grouping;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.AggNameInput;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.util.CollectionUtils;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * A type of {@code Function} that creates groups or buckets.
 */
public abstract class GroupingFunction extends Function {

    private final Expression field;
    private final List<Expression> extraChildren;
    private final List<Expression> parameters;

    private GroupingFunctionAttribute lazyAttribute;

    protected GroupingFunction(Source source, Expression field, List<Expression> extraChildren) {
        this(source, field, extraChildren, emptyList());
    }

    protected GroupingFunction(Source source, Expression field, List<Expression> extraChildren, List<Expression> parameters) {
        super(source, CollectionUtils.combine(singletonList(field), extraChildren, parameters));
        this.field = field;
        this.extraChildren = extraChildren == null ? emptyList() : extraChildren;
        this.parameters = parameters;
    }

    public Expression field() {
        return field;
    }
    
    public List<Expression> extraChildren() {
        return extraChildren;
    }

    public List<Expression> parameters() {
        return parameters;
    }

    @Override
    public GroupingFunctionAttribute toAttribute() {
        if (lazyAttribute == null) {
            // this is highly correlated with QueryFolder$FoldAggregate#addAggFunction (regarding the function name within the querydsl)
            lazyAttribute = new GroupingFunctionAttribute(source(), name(), dataType(), id(), functionId());
        }
        return lazyAttribute;
    }

    @Override
    protected Pipe makePipe() {
        // unresolved AggNameInput (should always get replaced by the folder)
        return new AggNameInput(source(), this, name());
    }

    @Override
    public ScriptTemplate asScript() {
        throw new SqlIllegalArgumentException("Grouping functions cannot be scripted");
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        GroupingFunction other = (GroupingFunction) obj;
        return Objects.equals(other.field(), field())
            && Objects.equals(other.extraChildren(), extraChildren())
            && Objects.equals(other.parameters(), parameters());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), extraChildren(), parameters());
    }
}