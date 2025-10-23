/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

public class UnresolvedTimestamp extends UnresolvedAttribute {

    private static final String NAME = "@timestamp";

    public static final UnresolvedTimestamp INSTANCE = new UnresolvedTimestamp();

    private UnresolvedTimestamp() {
        super(Source.EMPTY, NAME);
    }

    @Override
    public final DataType dataType() {
        return DataType.DATETIME;
    }
}
