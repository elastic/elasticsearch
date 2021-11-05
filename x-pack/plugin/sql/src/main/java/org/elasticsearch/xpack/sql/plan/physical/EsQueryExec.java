/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plan.physical;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.execution.search.Querier;
import org.elasticsearch.xpack.sql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.session.SqlSession;

import java.util.List;
import java.util.Objects;

public class EsQueryExec extends LeafExec {

    private final String index;
    private final List<Attribute> output;
    private final QueryContainer queryContainer;

    public EsQueryExec(Source source, String index, List<Attribute> output, QueryContainer queryContainer) {
        super(source);
        this.index = index;
        this.output = output;
        this.queryContainer = queryContainer;
    }

    @Override
    protected NodeInfo<EsQueryExec> info() {
        return NodeInfo.create(this, EsQueryExec::new, index, output, queryContainer);
    }

    public EsQueryExec with(QueryContainer queryContainer) {
        return new EsQueryExec(source(), index, output, queryContainer);
    }

    public String index() {
        return index;
    }

    public QueryContainer queryContainer() {
        return queryContainer;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public void execute(SqlSession session, ActionListener<Page> listener) {
        Querier scroller = new Querier(session);

        scroller.query(output, queryContainer, index, listener);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, queryContainer, output);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        EsQueryExec other = (EsQueryExec) obj;
        return Objects.equals(index, other.index)
            && Objects.equals(queryContainer, other.queryContainer)
            && Objects.equals(output, other.output);
    }

    @Override
    public String nodeString() {
        return nodeName() + "[" + index + "," + queryContainer + "]";
    }
}
