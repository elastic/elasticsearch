/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.capabilities.Unresolvable;
import org.elasticsearch.xpack.sql.capabilities.UnresolvedException;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.CollectionUtils;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static java.lang.String.format;

// unfortunately we can't use UnresolvedNamedExpression
public class UnresolvedAttribute extends Attribute implements Unresolvable {

    private final String unresolvedMsg;
    private final boolean customMessage;
    private final Object resolutionMetadata;

    public UnresolvedAttribute(Source source, String name) {
        this(source, name, null);
    }

    public UnresolvedAttribute(Source source, String name, String qualifier) {
        this(source, name, qualifier, null);
    }

    public UnresolvedAttribute(Source source, String name, String qualifier, String unresolvedMessage) {
        this(source, name, qualifier, null, unresolvedMessage, null);
    }

    public UnresolvedAttribute(Source source, String name, String qualifier, ExpressionId id, String unresolvedMessage,
            Object resolutionMetadata) {
        super(source, name, qualifier, id);
        this.customMessage = unresolvedMessage != null;
        this.unresolvedMsg = unresolvedMessage == null ? errorMessage(qualifiedName(), null) : unresolvedMessage;
        this.resolutionMetadata = resolutionMetadata;
    }

    @Override
    protected NodeInfo<UnresolvedAttribute> info() {
        return NodeInfo.create(this, UnresolvedAttribute::new,
            name(), qualifier(), id(), unresolvedMsg, resolutionMetadata);
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
    protected Attribute clone(Source source, String name, String qualifier, Nullability nullability,
                              ExpressionId id, boolean synthetic) {
        return this;
    }

    public UnresolvedAttribute withUnresolvedMessage(String unresolvedMsg) {
        return new UnresolvedAttribute(source(), name(), qualifier(), id(), unresolvedMsg, resolutionMetadata());
    }

    @Override
    public DataType dataType() {
        throw new UnresolvedException("dataType", this);
    }

    @Override
    public String nodeString() {
        return format(Locale.ROOT, "unknown column '%s'", name());
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
    public String unresolvedMessage() {
        return unresolvedMsg;
    }

    public static String errorMessage(String name, List<String> potentialMatches) {
        String msg = "Unknown column [" + name + "]";
        if (!CollectionUtils.isEmpty(potentialMatches)) {
            msg += ", did you mean " + (potentialMatches.size() == 1 ? "[" + potentialMatches.get(0)
                    + "]": "any of " + potentialMatches.toString()) + "?";
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
