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

import java.util.List;

public class UnresolvedTimestamp extends UnresolvedAttribute {

    public static final String UNRESOLVED_SUFFIX = "requires the ["
        + MetadataAttribute.TIMESTAMP_FIELD
        + "] field, which was either not present in the source index, or has been dropped"
        + " or renamed"; // TODO: track the name change

    public UnresolvedTimestamp(Source source) {
        this(source, null, MetadataAttribute.TIMESTAMP_FIELD, null, null);
    }

    public UnresolvedTimestamp(Source source, String qualifier, String name, NameId id, String unresolvedMessage) {
        super(source, qualifier, name, id, unresolvedMessage);
    }

    @Override
    protected NodeInfo<UnresolvedTimestamp> info() {
        return NodeInfo.create(this, UnresolvedTimestamp::new, qualifier(), name(), id(), super.unresolvedMessage());
    }

    @Override
    public UnresolvedTimestamp withUnresolvedMessage(String unresolvedMessage) {
        return new UnresolvedTimestamp(source(), qualifier(), name(), id(), unresolvedMessage);
    }

    @Override
    public String defaultUnresolvedMessage(@Nullable List<String> ignored) {
        return "[" + sourceText() + "] " + UNRESOLVED_SUFFIX;
    }

    public static UnresolvedTimestamp withSource(Source source) {
        return new UnresolvedTimestamp(source);
    }
}
