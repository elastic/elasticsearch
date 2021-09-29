/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.expression;

import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

public class OptionalUnresolvedAttribute extends UnresolvedAttribute {

    private Boolean resolved = null;

    public OptionalUnresolvedAttribute(Source source, String name) {
        super(source, name);
    }

    @Override
    public boolean resolved() {
        return resolved == null ? false : resolved;
    }

    public void markAsResolved() {
        resolved = Boolean.TRUE;
    }

    @Override
    public DataType dataType() {
        return DataTypes.NULL;
    }
}
