/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.tree.Source;

public class UnresolvedRemovedAttribute extends UnresolvedAttribute {
    public UnresolvedRemovedAttribute(Source source, String name) {
        super(source, name);
    }
}
