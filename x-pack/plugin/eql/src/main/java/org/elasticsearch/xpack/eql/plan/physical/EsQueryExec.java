/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plan.physical;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.eql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.session.Results;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

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

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public void execute(EqlSession session, ActionListener<Results> listener) {
        throw new UnsupportedOperationException();
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