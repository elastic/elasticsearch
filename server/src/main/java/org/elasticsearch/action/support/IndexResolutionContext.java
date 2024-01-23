/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

public record IndexResolutionContext(IndicesOptions delegate) {
    /**
     * @return True if the requested indices should include open indices.
     */
    public boolean includeOpen() {
        return delegate.expandWildcardsOpen();
    }

    /**
     * @return True if the requested indices should include closed indices.
     */
    public boolean includeClosed() {
        return delegate.expandWildcardsClosed();
    }

    /**
     * @return If this is false, the indices options prevent any indices from being resolved.
     */
    public boolean anyIndicesIncluded() {
        return includeOpen() || includeClosed();
    }

    /**
     * @return True if wildcards should be expanded to include aliases, and then those aliases resolved to concrete
     * indices. False if aliases should be ignored.
     */
    public boolean resolveAliases() {
        return delegate.ignoreAliases() == false;
    }

    /**
     * @return True if throttled indices should be removed from the final list, false if they should be included.
     */
    public boolean removeThrottled() {
        return delegate.ignoreThrottled();
    }

    /**
     * @return True if hidden indices should be removed from the final list, false if they should be included.
     */
    public boolean removeHidden() {
        return delegate.expandWildcardsHidden() == false;
    }

    /**
     * @return If this is false, an exception will be thrown if any of the resolved indices are unavailable.
     */
    public boolean allowUnavailable() {
        return delegate.ignoreUnavailable();
    }

    /**
     * @return If this is false, an exception will be thrown if any of the resolved indices are closed.
     */
    public boolean allowClosedIndices() {
        return delegate.forbidClosedIndices() == false;
    }

    /**
     * @return If this is false, an exception will be thrown if the list of resolved indices is empty.
     */
    public boolean allowNoIndices() {
        return delegate.allowNoIndices();
    }

    /**
     * @return If this is false, an exception will be thrown if the alias is resolved to multiple indices.
     */
    public boolean allowAliasesToMultipleIndices() {
        return delegate.allowAliasesToMultipleIndices();
    }

}
