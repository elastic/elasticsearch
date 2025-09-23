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

/**
 * An unresolved attribute. We build these while walking the syntax
 * tree and then resolve them into other {@link Attribute} subclasses during
 * analysis.
 * <p>
 *     For example, if they reference the data directly from lucene they'll be
 *     {@link FieldAttribute}s. If they reference the results of another calculation
 *     they will be {@link ReferenceAttribute}s.
 * </p>
 */
public class UnresolvedAttribute extends Attribute implements Unresolvable {
    private final boolean customMessage;
    private final String unresolvedMsg;
    private final Object resolutionMetadata;

    // TODO: Check usage of constructors without qualifiers, that's likely where qualifiers need to be plugged into resolution logic.
    public UnresolvedAttribute(Source source, String name) {
        this(source, name, null);
    }

    public UnresolvedAttribute(Source source, String name, @Nullable String unresolvedMessage) {
        this(source, null, name, unresolvedMessage);
    }

    public UnresolvedAttribute(Source source, @Nullable String qualifier, String name, @Nullable String unresolvedMessage) {
        this(source, qualifier, name, null, unresolvedMessage, null);
    }

    public UnresolvedAttribute(
        Source source,
        String name,
        @Nullable NameId id,
        @Nullable String unresolvedMessage,
        Object resolutionMetadata
    ) {
        this(source, null, name, id, unresolvedMessage, resolutionMetadata);
    }

    @SuppressWarnings("this-escape")
    public UnresolvedAttribute(
        Source source,
        @Nullable String qualifier,
        String name,
        @Nullable NameId id,
        @Nullable String unresolvedMessage,
        Object resolutionMetadata
    ) {
        super(source, qualifier, name, id);
        this.customMessage = unresolvedMessage != null;
        this.unresolvedMsg = unresolvedMessage == null
            ? errorMessage(qualifier() != null ? qualifiedName() : name(), null)
            : unresolvedMessage;
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
        return NodeInfo.create(this, UnresolvedAttribute::new, qualifier(), name(), id(), unresolvedMsg, resolutionMetadata);
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
    protected Attribute clone(
        Source source,
        String qualifier,
        String name,
        DataType dataType,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        // TODO: This looks like a bug; making clones should allow for changes.
        return this;
    }

    public UnresolvedAttribute withUnresolvedMessage(String unresolvedMessage) {
        return new UnresolvedAttribute(source(), qualifier(), name(), id(), unresolvedMessage, resolutionMetadata());
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
        return UNRESOLVED_PREFIX + qualifiedName();
    }

    @Override
    protected String label() {
        return UNRESOLVED_PREFIX;
    }

    @Override
    public boolean isDimension() {
        // We don't want to use this during the analysis phase, and this class does not exist after analysis
        throw new UnsupportedOperationException("This should never be called before the attribute is resolved");
    }

    @Override
    public boolean isMetric() {
        // We don't want to use this during the analysis phase, and this class does not exist after analysis
        throw new UnsupportedOperationException("This should never be called before the attribute is resolved");
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
    @SuppressWarnings("checkstyle:EqualsHashCode")// equals is implemented in parent. See innerEquals instead
    public int hashCode() {
        return Objects.hash(super.hashCode(), resolutionMetadata, unresolvedMsg);
    }

    @Override
    protected boolean innerEquals(Object o) {
        var other = (UnresolvedAttribute) o;
        return super.innerEquals(other)
            && Objects.equals(resolutionMetadata, other.resolutionMetadata)
            && Objects.equals(unresolvedMsg, other.unresolvedMsg);
    }
}
