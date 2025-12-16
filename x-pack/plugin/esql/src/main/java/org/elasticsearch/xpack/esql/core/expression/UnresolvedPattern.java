/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

public class UnresolvedPattern extends UnresolvedAttribute {
    public UnresolvedPattern(Source source, String name) {
        super(source, name);
    }

    public UnresolvedPattern(
        Source source,
        @Nullable String qualifier,
        String name,
        @Nullable NameId id,
        @Nullable String unresolvedMessage
    ) {
        super(source, qualifier, name, id, unresolvedMessage);
    }

    @Override
    public UnresolvedPattern withUnresolvedMessage(String unresolvedMessage) {
        return new UnresolvedPattern(source(), qualifier(), name(), id(), unresolvedMessage);
    }

    @Override
    protected NodeInfo<? extends UnresolvedPattern> info() {
        return NodeInfo.create(this, UnresolvedPattern::new, qualifier(), name(), id(), unresolvedMessage());
    }
}
