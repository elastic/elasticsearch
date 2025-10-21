/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

public class UnresolvedTimestamp extends UnresolvedAttribute {
    public UnresolvedTimestamp(Source source, String name) {
        super(source, name);
    }

    public UnresolvedTimestamp(Source source, String name, String unresolvedMessage) {
        super(source, name, unresolvedMessage);
    }

    public UnresolvedTimestamp(Source source, String qualifier, String name, String unresolvedMessage) {
        super(source, qualifier, name, unresolvedMessage);
    }

    public UnresolvedTimestamp(Source source, String name, NameId id, String unresolvedMessage, Object resolutionMetadata) {
        super(source, name, id, unresolvedMessage, resolutionMetadata);
    }

    public UnresolvedTimestamp(
        Source source,
        String qualifier,
        String name,
        NameId id,
        String unresolvedMessage,
        Object resolutionMetadata
    ) {
        super(source, qualifier, name, id, unresolvedMessage, resolutionMetadata);
    }

    @Override
    public String unresolvedMessage() {
        String parentUnreslovedMsg = super.unresolvedMessage();
        if (parentUnreslovedMsg != null) {
            return "Rate aggregation requires @timestamp field, but @timestamp was renamed or dropped";
        }
        return null;
    }

    @Override
    protected NodeInfo<UnresolvedAttribute> info() {
        return NodeInfo.create(this, UnresolvedTimestamp::new, qualifier(), name(), id(), unresolvedMsg, resolutionMetadata);
    }
}
