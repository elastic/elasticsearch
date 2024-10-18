/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.fulltext;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

public final class StringQueryPredicate extends FullTextPredicate {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StringQueryPredicate",
        StringQueryPredicate::new
    );

    private final Map<String, Float> fields;

    public StringQueryPredicate(Source source, String query, String options) {
        super(source, query, options, emptyList());

        // inferred
        this.fields = FullTextUtils.parseFields(optionMap(), source);
    }

    StringQueryPredicate(StreamInput in) throws IOException {
        super(in);
        assert super.children().isEmpty();
        this.fields = FullTextUtils.parseFields(optionMap(), source());
    }

    @Override
    protected NodeInfo<StringQueryPredicate> info() {
        return NodeInfo.create(this, StringQueryPredicate::new, query(), options());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
    }

    public Map<String, Float> fields() {
        return fields;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }
}
