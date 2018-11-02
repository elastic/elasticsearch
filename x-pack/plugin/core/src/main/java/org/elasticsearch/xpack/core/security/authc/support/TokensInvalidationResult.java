/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * The result of attempting to invalidate one or multiple tokens. The result contains information about:
 * <ul>
 * <li>which and how many of the tokens were actually invalidated</li>
 * <li>which and how many tokens are not invalidated in this request because they were already invalidated</li>
 * <li>which and how many tokens were not invalidated because of an error</li>
 * </ul>
 */
public class TokensInvalidationResult {

    private final Collection<String> invalidatedTokens;
    private final Collection<String> notInvalidatedTokens;
    private final Collection<Tuple<String, String>> errors;


    public TokensInvalidationResult(Collection<String> invalidatedTokens, Collection<String> notInvalidatedTokens,
                                    @Nullable Collection<Tuple<String, String>> errors) {
        Objects.requireNonNull(invalidatedTokens, "invalidated_tokens must be provided");
        this.invalidatedTokens = invalidatedTokens;
        Objects.requireNonNull(notInvalidatedTokens, "not_invalidated_must be provided");
        this.notInvalidatedTokens = notInvalidatedTokens;
        if (null != errors) {
            this.errors = errors;
        } else {
            this.errors = Collections.emptyList();
        }
    }


    public Collection<String> getInvalidatedTokens() {
        return invalidatedTokens;
    }

    public Collection<String> getNotInvalidatedTokens() {
        return notInvalidatedTokens;
    }

    public Collection<Tuple<String, String>> getErrors() {
        return errors;
    }

}
