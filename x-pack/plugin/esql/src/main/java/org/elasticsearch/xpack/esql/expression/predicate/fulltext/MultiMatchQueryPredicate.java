/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.fulltext;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.MultiMatchQuery;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class MultiMatchQueryPredicate extends FullTextPredicate implements TranslationAware {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MultiMatchQueryPredicate",
        MultiMatchQueryPredicate::new
    );

    private final String fieldString;
    private final Map<String, Float> fields;

    public MultiMatchQueryPredicate(Source source, String fieldString, String query, String options) {
        super(source, query, options, emptyList());
        this.fieldString = fieldString;
        // inferred
        this.fields = FullTextUtils.parseFields(fieldString, source);
    }

    MultiMatchQueryPredicate(StreamInput in) throws IOException {
        super(in);
        assert super.children().isEmpty();
        fieldString = in.readString();
        // inferred
        this.fields = FullTextUtils.parseFields(fieldString, source());
    }

    @Override
    protected NodeInfo<MultiMatchQueryPredicate> info() {
        return NodeInfo.create(this, MultiMatchQueryPredicate::new, fieldString, query(), options());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
    }

    public String fieldString() {
        return fieldString;
    }

    public Map<String, Float> fields() {
        return fields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(fieldString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldString, super.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            MultiMatchQueryPredicate other = (MultiMatchQueryPredicate) obj;
            return Objects.equals(fieldString, other.fieldString);
        }
        return false;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        return Translatable.YES; // needs update if we'll ever validate the fields
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        return new MultiMatchQuery(source(), query(), fields(), this);
    }
}
