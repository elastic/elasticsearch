/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.fulltext;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class MultiMatchQueryPredicate extends FullTextPredicate {

    private final String fieldString;
    private final Map<String, Float> fields;

    public MultiMatchQueryPredicate(Source source, String fieldString, String query, String options) {
        super(source, query, options, emptyList());
        this.fieldString = fieldString;
        // inferred
        this.fields = FullTextUtils.parseFields(fieldString, source);
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
}
