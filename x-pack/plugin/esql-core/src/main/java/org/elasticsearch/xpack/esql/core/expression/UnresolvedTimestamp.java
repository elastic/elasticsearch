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
import java.util.Objects;

public class UnresolvedTimestamp extends UnresolvedAttribute {
    private final String errorMessage;

    public UnresolvedTimestamp(Source source, String errorMessage) {
        this(source, null, MetadataAttribute.TIMESTAMP_FIELD, null, null, null, errorMessage);
    }

    public UnresolvedTimestamp(
        Source source,
        String qualifier,
        String name,
        NameId id,
        String unresolvedMessage,
        Object resolutionMetadata,
        String errorMessage
    ) {
        super(source, qualifier, name, id, unresolvedMessage, resolutionMetadata);
        this.errorMessage = errorMessage;
    }

    @Override
    protected NodeInfo<UnresolvedTimestamp> info() {
        return NodeInfo.create(
            this,
            UnresolvedTimestamp::new,
            qualifier(),
            name(),
            id(),
            super.unresolvedMessage(),
            resolutionMetadata(),
            errorMessage
        );
    }

    @Override
    public UnresolvedTimestamp withUnresolvedMessage(String unresolvedMessage) {
        return new UnresolvedTimestamp(source(), qualifier(), name(), id(), unresolvedMessage, resolutionMetadata(), errorMessage);
    }

    @Override
    public String unresolvedMessage() {
        return errorMessage;
    }

    @Override
    @SuppressWarnings("checkstyle:EqualsHashCode")// equals is implemented in parent. See innerEquals instead
    public int hashCode() {
        return Objects.hash(super.hashCode(), errorMessage);
    }

    @Override
    protected boolean innerEquals(Object o) {
        var other = (UnresolvedTimestamp) o;
        return super.innerEquals(other) && Objects.equals(errorMessage, other.errorMessage);
    }
}
