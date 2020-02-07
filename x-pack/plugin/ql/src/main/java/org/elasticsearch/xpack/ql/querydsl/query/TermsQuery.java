/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypeConverter;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

public class TermsQuery extends LeafQuery {

    private final String term;
    private final Set<Object> values;

    public TermsQuery(Source source, String term, List<Expression> values) {
        super(source);
        this.term = term;
        values.removeIf(e -> DataTypes.isNull(e.dataType()));
        if (values.isEmpty()) {
            this.values = Collections.emptySet();
        } else {
            DataType dt = values.get(0).dataType();
            Set<Object> set = new LinkedHashSet<>(CollectionUtils.mapSize(values.size()));
            for (Expression e : values) {
                if (e.foldable()) {
                    set.add(DataTypeConverter.convert(e.fold(), dt));
                } else {
                    throw new QlIllegalArgumentException("Cannot determine value for {}", e);
                }
            }
            this.values = set;
        }
    }

    @Override
    public QueryBuilder asBuilder() {
        return termsQuery(term, values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, values);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TermsQuery other = (TermsQuery) obj;
        return Objects.equals(term, other.term)
            && Objects.equals(values, other.values);
    }

    @Override
    protected String innerToString() {
        return term + ":" + values;
    }
}