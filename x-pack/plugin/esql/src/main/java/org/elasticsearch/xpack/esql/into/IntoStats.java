/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

public record IntoStats(long total, long created) {
    public static final List<Attribute> ATTRIBUTES = List.of(
        new ReferenceAttribute(Source.EMPTY, null, "total", DataType.LONG),
        new ReferenceAttribute(Source.EMPTY, null, "created", DataType.LONG)
    );

    public Page toPage(BlockFactory blockFactory) {
        return new Page(BlockUtils.fromArrayRow(blockFactory, total, created));
    }
}
