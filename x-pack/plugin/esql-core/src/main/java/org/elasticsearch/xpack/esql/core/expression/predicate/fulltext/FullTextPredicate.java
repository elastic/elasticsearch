/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.fulltext;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class FullTextPredicate extends Expression {

    public enum Operator {
        AND,
        OR;

        public org.elasticsearch.index.query.Operator toEs() {
            return org.elasticsearch.index.query.Operator.fromString(name());
        }
    }

    private final String query;
    private final String options;
    private final Map<String, String> optionMap;
    // common properties
    private final String analyzer;

    FullTextPredicate(Source source, String query, @Nullable String options, List<Expression> children) {
        super(source, children);
        this.query = query;
        this.options = options;
        // inferred
        this.optionMap = FullTextUtils.parseSettings(options, source);
        this.analyzer = optionMap.get("analyzer");
    }

    protected FullTextPredicate(StreamInput in) throws IOException {
        this(
            Source.readFrom((StreamInput & PlanStreamInput) in),
            in.readString(),
            in.readOptionalString(),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    public String query() {
        return query;
    }

    public String options() {
        return options;
    }

    public Map<String, String> optionMap() {
        return optionMap;
    }

    public String analyzer() {
        return analyzer;
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeString(query);
        out.writeOptionalString(options);
        out.writeNamedWriteableCollection(children());
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, options);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        FullTextPredicate other = (FullTextPredicate) obj;
        return Objects.equals(query, other.query) && Objects.equals(options, other.options);
    }
}
