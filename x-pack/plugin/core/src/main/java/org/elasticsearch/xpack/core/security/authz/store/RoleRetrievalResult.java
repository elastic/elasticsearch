/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.store;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.Objects;
import java.util.Set;

/**
 * The result of attempting to retrieve roles from a roles provider. The result can either be
 * successful or a failure. A successful result indicates that no errors occurred while retrieving
 * roles, even if none of the requested roles could be found. A failure indicates an error
 * occurred while retrieving the results but the error is not fatal and the request may be able
 * to continue.
 */
public final class RoleRetrievalResult {

    private final Set<RoleDescriptor> descriptors;

    @Nullable
    private final Exception failure;

    private RoleRetrievalResult(Set<RoleDescriptor> descriptors, @Nullable Exception failure) {
        if (descriptors != null && failure != null) {
            throw new IllegalArgumentException("either descriptors or failure must be null");
        }
        this.descriptors = descriptors;
        this.failure = failure;
    }

    /**
     * @return the resolved descriptors or {@code null} if there was a failure
     */
    public Set<RoleDescriptor> getDescriptors() {
        return descriptors;
    }

    /**
     * @return the failure or {@code null} if retrieval succeeded
     */
    @Nullable
    public Exception getFailure() {
        return failure;
    }

    /**
     * @return true if the retrieval succeeded
     */
    public boolean isSuccess() {
        return descriptors != null;
    }

    /**
     * Creates a successful result with the provided {@link RoleDescriptor} set,
     * which must be non-null
     */
    public static RoleRetrievalResult success(Set<RoleDescriptor> descriptors) {
        Objects.requireNonNull(descriptors, "descriptors must not be null if successful");
        return new RoleRetrievalResult(descriptors, null);
    }

    /**
     * Creates a failed result with the provided non-null exception
     */
    public static RoleRetrievalResult failure(Exception e) {
        Objects.requireNonNull(e, "Exception must be provided");
        return new RoleRetrievalResult(null, e);
    }
}
