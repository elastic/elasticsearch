/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.plan.physical;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.eql.execution.search.AsEventListener;
import org.elasticsearch.xpack.eql.execution.search.BasicQueryClient;
import org.elasticsearch.xpack.eql.execution.search.QueryRequest;
import org.elasticsearch.xpack.eql.execution.search.ReverseListener;
import org.elasticsearch.xpack.eql.execution.search.SourceGenerator;
import org.elasticsearch.xpack.eql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.eql.session.EqlConfiguration;
import org.elasticsearch.xpack.eql.session.EqlSession;
import org.elasticsearch.xpack.eql.session.Payload;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class EsQueryExec extends LeafExec {

    private final List<Attribute> output;
    private final QueryContainer queryContainer;

    public EsQueryExec(Source source, List<Attribute> output, QueryContainer queryContainer) {
        super(source);
        this.output = output;
        this.queryContainer = queryContainer;
    }

    @Override
    protected NodeInfo<EsQueryExec> info() {
        return NodeInfo.create(this, EsQueryExec::new, output, queryContainer);
    }

    public EsQueryExec with(QueryContainer queryContainer) {
        return new EsQueryExec(source(), output, queryContainer);
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    /*
     * {@param includeFetchFields} should be true for event queries and false for in progress sequence queries
     * Fetching fields during in progress sequence queries is unnecessary.
     */
    public SearchSourceBuilder source(EqlSession session, boolean includeFetchFields) {
        EqlConfiguration cfg = session.configuration();
        // by default use the configuration size
        return SourceGenerator.sourceBuilder(
            queryContainer,
            cfg.filter(),
            includeFetchFields ? cfg.fetchFields() : null,
            cfg.runtimeMappings()
        );
    }

    @Override
    public void execute(EqlSession session, ActionListener<Payload> listener) {
        // endpoint - fetch all source
        QueryRequest request = () -> source(session, true).fetchSource(FetchSourceContext.FETCH_SOURCE);
        listener = shouldReverse(request) ? new ReverseListener(listener) : listener;
        new BasicQueryClient(session).query(request, new AsEventListener(listener));
    }

    private boolean shouldReverse(QueryRequest query) {
        SearchSourceBuilder searchSource = query.searchSource();
        // since all results need to be ASC, use this hack to figure out whether the results need to be flipped
        for (SortBuilder<?> sort : searchSource.sorts()) {
            if (sort.order() == SortOrder.DESC) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryContainer, output);
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
        return Objects.equals(queryContainer, other.queryContainer) && Objects.equals(output, other.output);
    }

    @Override
    public String nodeString() {
        return nodeName() + "[" + queryContainer + "]";
    }

    public QueryContainer queryContainer() {
        return queryContainer;
    }
}
