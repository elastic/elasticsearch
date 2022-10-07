/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedStar;
import org.elasticsearch.xpack.ql.tree.Source;

public class UnresolvedStarAttribute extends UnresolvedStar {
    public UnresolvedStarAttribute(Source source, UnresolvedAttribute qualifier) {
        super(source, qualifier);
    }

    @Override
    public String unresolvedMessage() {
        return "Cannot determine columns for [" + message() + "]";
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + message();
    }

    private String message() {
        return qualifier() != null ? qualifier().qualifiedName() : "*";
    }
}
