/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.fulltext;

import java.util.Map;
import java.util.List;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import static java.util.Collections.emptyList;

public class StringQueryPredicate extends FullTextPredicate {

    private final Map<String, Float> fields;

    public StringQueryPredicate(Source source, String query, String options) {
        super(source, query, options, emptyList());

        // inferred
        this.fields = FullTextUtils.parseFields(optionMap(), source);
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
}
