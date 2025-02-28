/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.hybrid;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilderWrapper;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;

import java.util.Objects;

public class StandardRetrieverBuilderWrapper extends RetrieverBuilderWrapper<StandardRetrieverBuilderWrapper> {
    private final String field;

    public StandardRetrieverBuilderWrapper(String field) {
        super(new StandardRetrieverBuilder(new BoolQueryBuilder()));

        Objects.requireNonNull(field);
        this.field = field;
    }

    private StandardRetrieverBuilderWrapper(String field, RetrieverBuilder retrieverBuilder) {
        super(retrieverBuilder);
        this.field = field;
    }

    @Override
    protected StandardRetrieverBuilderWrapper clone(RetrieverBuilder sub) {
        return new StandardRetrieverBuilderWrapper(field, sub);
    }

    @Override
    protected boolean doEquals(Object o) {
        StandardRetrieverBuilderWrapper that = (StandardRetrieverBuilderWrapper) o;
        return field.equals(that.field) && super.doEquals(o);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, super.doHashCode());
    }
}
