/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.capabilities.UnresolvedException;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

// unfortunately we can't use UnresolvedNamedExpression
public class UnresolvedAttribute extends Attribute implements Unresolvable {
    private final String unresolvedMsg;
    private final boolean customMessage;
    private final Object resolutionMetadata;

    public UnresolvedAttribute(Source source, String name) {
        this(source, name, null);
    }

    public UnresolvedAttribute(Source source, String name, String unresolvedMessage) {
        this(source, name, null, unresolvedMessage, null);
    }

    @SuppressWarnings("this-escape")
    public UnresolvedAttribute(Source source, String name, @Nullable NameId id, String unresolvedMessage, Object resolutionMetadata) {
        super(source, name, id);
        this.customMessage = unresolvedMessage != null;
        this.unresolvedMsg = unresolvedMessage == null ? errorMessage(name(), null) : unresolvedMessage;
        this.resolutionMetadata = resolutionMetadata;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    @Override
    protected NodeInfo<UnresolvedAttribute> info() {
        return NodeInfo.create(this, UnresolvedAttribute::new, name(), id(), unresolvedMsg, resolutionMetadata);
    }

    public Object resolutionMetadata() {
        return resolutionMetadata;
    }

    public boolean customMessage() {
        return customMessage;
    }

    @Override
    public boolean resolved() {
        return false;
    }

    @Override
    protected Attribute clone(Source source, String name, DataType dataType, Nullability nullability, NameId id, boolean synthetic) {
        return this;
    }

    public UnresolvedAttribute withUnresolvedMessage(String unresolvedMessage) {
        return new UnresolvedAttribute(source(), name(), id(), unresolvedMessage, resolutionMetadata());
    }

    @Override
    protected TypeResolution resolveType() {
        return new TypeResolution("unresolved attribute [" + name() + "]");
    }

    @Override
    public DataType dataType() {
        throw new UnresolvedException("dataType", this);
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + name();
    }

    @Override
    protected String label() {
        return UNRESOLVED_PREFIX;
    }

    @Override
    public String nodeString() {
        return toString();
    }

    @Override
    public String unresolvedMessage() {
        return unresolvedMsg;
    }

    public static String errorMessage(String name, List<String> potentialMatches) {
        String msg = "Unknown column [" + name + "]";
        if (CollectionUtils.isEmpty(potentialMatches) == false) {
            msg += ", did you mean "
                + (potentialMatches.size() == 1 ? "[" + potentialMatches.get(0) + "]" : "any of " + potentialMatches.toString())
                + "?";
        }
        return msg;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resolutionMetadata, unresolvedMsg);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            UnresolvedAttribute ua = (UnresolvedAttribute) obj;
            return Objects.equals(resolutionMetadata, ua.resolutionMetadata) && Objects.equals(unresolvedMsg, ua.unresolvedMsg);
        }
        return false;
    }
}
