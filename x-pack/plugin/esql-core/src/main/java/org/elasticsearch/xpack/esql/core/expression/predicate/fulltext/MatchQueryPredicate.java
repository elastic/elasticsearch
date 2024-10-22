/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.fulltext;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
    private final Double boost;
    private final Fuzziness fuzziness;

    public MatchQueryPredicate(Source source, Expression field, String query, String options) {
        super(source, query, options, singletonList(field));
        this.field = field;
        this.boost = null;
        this.fuzziness = null;
    }

    public MatchQueryPredicate(Source source, Expression field, String query, Double boost, Fuzziness fuzziness) {
        super(source, query, createOptions(boost, fuzziness), singletonList(field));
        this.field = field;
        this.boost = boost;
        this.fuzziness = fuzziness;
    }

    private static String createOptions(Double boost, Fuzziness fuzziness) {
        StringBuilder options = new StringBuilder();
        if (boost != null) {
            options.append(BOOST_OPTION).append("=").append(boost);
        }
        if (fuzziness != null) {
            if (boost != null) {
                options.append(";");
            }
            options.append(FUZZINESS_OPTION).append("=").append(fuzziness.asString());
        }
        return options.toString();
    }

    MatchQueryPredicate(StreamInput in) throws IOException {
        super(in);
        assert super.children().size() == 1;
        field = super.children().get(0);
        if (TransportVersions.MATCH_OPERATOR_FUZZINESS_BOOSTING.onOrAfter(in.getTransportVersion())) {
            boost = in.readOptionalDouble();
            fuzziness = in.readOptionalWriteable(Fuzziness::new);
        } else {
            boost = null;
            fuzziness = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (TransportVersions.MATCH_OPERATOR_FUZZINESS_BOOSTING.onOrAfter(out.getTransportVersion())) {
            out.writeOptionalDouble(boost);
            out.writeOptionalWriteable(fuzziness);
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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
        return Objects.hash(field, super.hashCode(), boost, fuzziness);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            MatchQueryPredicate other = (MatchQueryPredicate) obj;
            return Objects.equals(field, other.field)
                && Objects.equals(boost, other.boost)
                && Objects.equals(fuzziness, other.fuzziness);
        }
        return false;
    }

    public Double boost() {
        return boost;
    }

    public Fuzziness fuzziness() {
        return fuzziness;
    }
}
