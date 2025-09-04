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
import java.util.Objects;
import java.util.stream.Collectors;

// Represent flat -> canonical CPS rewrites
// Represent local index expansion

public final class ReplacedIndexExpression implements Writeable {
    public enum ResolutionResult {
        SUCCESS,
        CONCRETE_RESOURCE_UNAUTHORIZED,
        CONCRETE_RESOURCE_MISSING
    }

    private final String original;
    private final List<String> replacedBy;
    private final ResolutionResult resolutionResult;
    @Nullable
    private ElasticsearchException authorizationError;

    public ReplacedIndexExpression(StreamInput in) throws IOException {
        this.original = in.readString();
        this.replacedBy = in.readCollectionAsList(StreamInput::readString);
        this.resolutionResult = in.readEnum(ResolutionResult.class);
        this.authorizationError = ElasticsearchException.readException(in);
    }

    public ReplacedIndexExpression(
        String original,
        List<String> replacedBy,
        ResolutionResult resolutionResult,
        @Nullable ElasticsearchException exception
    ) {
        this.original = original;
        this.replacedBy = replacedBy;
        this.resolutionResult = resolutionResult;
        this.authorizationError = exception;
    }

    public ReplacedIndexExpression(String original, List<String> replacedBy) {
        this(original, replacedBy, ResolutionResult.SUCCESS, null);
    }

    public void setAuthorizationError(ElasticsearchException error) {
        assert resolutionResult == ResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED : "we should never set an error if we are authorized";
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

    public ResolutionResult resolutionResult() {
        return resolutionResult;
    }

    @Nullable
    public ElasticsearchException authorizationError() {
        return authorizationError;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(original);
        out.writeStringCollection(replacedBy);
        out.writeEnum(resolutionResult);
        ElasticsearchException.writeException(authorizationError, out);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ReplacedIndexExpression that = (ReplacedIndexExpression) o;
        return Objects.equals(original, that.original)
            && Objects.equals(replacedBy, that.replacedBy)
            && resolutionResult == that.resolutionResult
            && Objects.equals(authorizationError, that.authorizationError);
    }

    @Override
    public int hashCode() {
        return Objects.hash(original, replacedBy, resolutionResult, authorizationError);
    }

    @Override
    public String toString() {
        return "ReplacedIndexExpression{"
            + "original='"
            + original
            + '\''
            + ", replacedBy="
            + replacedBy
            + ", resolutionResult="
            + resolutionResult
            + ", authorizationError="
            + authorizationError
            + '}';
    }
}
