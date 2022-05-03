/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.function.aggregate;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.AggNameInput;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;

/**
 * A type of {@code Function} that takes multiple values and extracts a single value out of them. For example, {@code AVG()}.
 */
public abstract class AggregateFunction extends Function {

    private final Expression field;
    private final List<? extends Expression> parameters;

    protected AggregateFunction(Source source, Expression field) {
        this(source, field, emptyList());
    }

    protected AggregateFunction(Source source, Expression field, List<? extends Expression> parameters) {
        super(source, CollectionUtils.combine(singletonList(field), parameters));
        this.field = field;
        this.parameters = parameters;
    }

    public Expression field() {
        return field;
    }

    public List<? extends Expression> parameters() {
        return parameters;
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolutions.isExact(field, sourceText(), DEFAULT);
    }

    @Override
    protected Pipe makePipe() {
        // unresolved AggNameInput (should always get replaced by the folder)
        return new AggNameInput(source(), this, sourceText());
    }

    @Override
    public ScriptTemplate asScript() {
        throw new QlIllegalArgumentException("Aggregate functions cannot be scripted");
    }

    @Override
    public int hashCode() {
        // NB: the hashcode is currently used for key generation so
        // to avoid clashes between aggs with the same arguments, add the class name as variation
        return Objects.hash(getClass(), children());
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            AggregateFunction other = (AggregateFunction) obj;
            return Objects.equals(other.field(), field()) && Objects.equals(other.parameters(), parameters());
        }
        return false;
    }
}
