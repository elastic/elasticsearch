/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

public class UnresolvedTimestamp extends UnresolvedAttribute {

    public UnresolvedTimestamp(Source source) {
        super(source, MetadataAttribute.TIMESTAMP_FIELD);
    }

    @Override
    public final DataType dataType() {
        return DataType.DATETIME;
    }

    public static UnresolvedTimestamp withSource(Source source) {
        return new UnresolvedTimestamp(source);
    }

    @Override
    protected NodeInfo<UnresolvedTimestamp> info() {
        return NodeInfo.create(this);
    }
}
