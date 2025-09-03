/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class ReplacedIndexExpression implements Writeable {
    private final String original;
    private final List<String> replacedBy;
    private final boolean authorized;
    private final boolean existsAndVisible;
    @Nullable
    private ElasticsearchException authorizationError;

    public ReplacedIndexExpression(StreamInput in) throws IOException {
        this.original = in.readString();
        this.replacedBy = in.readCollectionAsList(StreamInput::readString);
        this.authorized = in.readBoolean();
        this.existsAndVisible = in.readBoolean();
        this.authorizationError = ElasticsearchException.readException(in);
    }

    public ReplacedIndexExpression(
        String original,
        List<String> replacedBy,
        boolean authorized,
        boolean existsAndVisible,
        @Nullable ElasticsearchException exception
    ) {
        this.original = original;
        this.replacedBy = replacedBy;
        this.authorized = authorized;
        this.existsAndVisible = existsAndVisible;
        this.authorizationError = exception;
    }

    public static String[] toIndices(Map<String, ReplacedIndexExpression> replacedExpressions) {
        return replacedExpressions.values()
            .stream()
            .flatMap(indexExpression -> indexExpression.replacedBy().stream())
            .toArray(String[]::new);
    }

    public ReplacedIndexExpression(String original, List<String> replacedBy) {
        this(original, replacedBy, true, true, null);
    }

    public void setAuthorizationError(ElasticsearchException error) {
        assert authorized == false : "we should never set an error if we are authorized";
        this.authorizationError = error;
    }

    public boolean hasLocalIndices() {
        return false == getLocalIndices().isEmpty();
    }

    public List<String> getLocalIndices() {
        return replacedBy.stream().filter(e -> false == RemoteClusterAware.isRemoteIndexName(e)).collect(Collectors.toList());
    }

    public List<String> getRemoteIndices() {
        return replacedBy.stream().filter(RemoteClusterAware::isRemoteIndexName).collect(Collectors.toList());
    }

    public String original() {
        return original;
    }

    public List<String> replacedBy() {
        return replacedBy;
    }

    public boolean authorized() {
        return authorized;
    }

    public boolean existsAndVisible() {
        return existsAndVisible;
    }

    @Nullable
    public ElasticsearchException authorizationError() {
        return authorizationError;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ReplacedIndexExpression that = (ReplacedIndexExpression) o;
        return authorized == that.authorized
            && existsAndVisible == that.existsAndVisible
            && Objects.equals(original, that.original)
            && Objects.equals(replacedBy, that.replacedBy)
            && Objects.equals(authorizationError, that.authorizationError);
    }

    @Override
    public int hashCode() {
        return Objects.hash(original, replacedBy, authorized, existsAndVisible, authorizationError);
    }

    @Override
    public String toString() {
        return "ReplacedExpression{"
            + "original='"
            + original
            + '\''
            + ", replacedBy="
            + replacedBy
            + ", authorized="
            + authorized
            + ", existsAndVisible="
            + existsAndVisible
            + ", authorizationError="
            + authorizationError
            + '}';
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(original);
        out.writeStringCollection(replacedBy);
        out.writeBoolean(authorized);
        out.writeBoolean(existsAndVisible);
        ElasticsearchException.writeException(authorizationError, out);
    }
}
