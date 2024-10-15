/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.fulltext;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.core.querydsl.query.MatchQuery.BOOST_OPTION;
import static org.elasticsearch.xpack.esql.core.querydsl.query.MatchQuery.FUZZINESS_OPTION;

public class MatchQueryPredicate extends FullTextPredicate {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MatchQueryPredicate",
        MatchQueryPredicate::new
    );

    private final Expression field;

    public MatchQueryPredicate(Source source, Expression field, String query, String options) {
        super(source, query, options, singletonList(field));
        this.field = field;
    }

    public MatchQueryPredicate(Source source, Expression field, String query, Float boost, Fuzziness fuzziness) {
        super(source, query, createOptions(boost, fuzziness), singletonList(field));
        this.field = field;
    }

    private static String createOptions(Float boost, Fuzziness fuzziness) {
        StringBuilder options = new StringBuilder();
        if (boost != null) {
            options.append(BOOST_OPTION).append("=").append(boost).append(";");
        }
        if (fuzziness != null) {
            options.append(FUZZINESS_OPTION).append("=").append(fuzziness.asString()).append(";");
        }
        return options.toString();
    }

    MatchQueryPredicate(StreamInput in) throws IOException {
        super(in);
        assert super.children().size() == 1;
        field = super.children().get(0);
    }

    @Override
    protected NodeInfo<MatchQueryPredicate> info() {
        return NodeInfo.create(this, MatchQueryPredicate::new, field, query(), options());
    }

    @Override
    public MatchQueryPredicate replaceChildren(List<Expression> newChildren) {
        return new MatchQueryPredicate(source(), newChildren.get(0), query(), options());
    }

    public Expression field() {
        return field;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, super.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            MatchQueryPredicate other = (MatchQueryPredicate) obj;
            return Objects.equals(field, other.field);
        }
        return false;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }
}
