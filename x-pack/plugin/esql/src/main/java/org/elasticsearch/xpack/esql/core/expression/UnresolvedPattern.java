/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;

/**
 * When a {@code KEEP} or a {@code DROP} receives a wildcard pattern, this is provided to them as an {@link UnresolvedNamePattern}. This
 * is run against the available attributes names. In case nothing matches, the resulting attribute would be an {@link UnresolvedAttribute},
 * which ends up being reported as to the user in a failure message. However, in case {@link UnmappedResolution unmapped fields} are
 * enabled, an {@link UnresolvedAttribute} isn't sufficient, as we the analyzer wouldn't know.
 */
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
