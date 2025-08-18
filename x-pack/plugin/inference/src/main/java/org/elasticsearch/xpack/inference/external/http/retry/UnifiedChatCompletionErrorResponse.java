/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.core.Nullable;

import java.util.Objects;

public class UnifiedChatCompletionErrorResponse extends ErrorResponse {
    public static final UnifiedChatCompletionErrorResponse UNDEFINED_ERROR = new UnifiedChatCompletionErrorResponse();

    @Nullable
    private final String code;
    @Nullable
    private final String param;
    private final String type;

    public UnifiedChatCompletionErrorResponse(String errorMessage, String type, @Nullable String code, @Nullable String param) {
        super(errorMessage);
        this.code = code;
        this.param = param;
        this.type = Objects.requireNonNull(type);
    }

    private UnifiedChatCompletionErrorResponse() {
        super(false);
        this.code = null;
        this.param = null;
        this.type = "unknown";
    }

    @Nullable
    public String code() {
        return code;
    }

    @Nullable
    public String param() {
        return param;
    }

    public String type() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        UnifiedChatCompletionErrorResponse that = (UnifiedChatCompletionErrorResponse) o;
        return Objects.equals(code, that.code) && Objects.equals(param, that.param) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), code, param, type);
    }
}
