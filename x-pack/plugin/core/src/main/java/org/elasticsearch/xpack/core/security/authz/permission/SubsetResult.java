/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.common.Nullable;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * This class is used to denote the result for role subset checks. The result
 * for subset checks returns {@link SubsetResult.Result#NO} if it is not a
 * subset , if it is clearly a subset will return
 * {@link SubsetResult.Result#YES} and it will return
 * {@link SubsetResult.Result#MAYBE} when the role is a subset in every other
 * aspect except DLS queries where we could modify the role by combining DLS
 * queries with base role so the result is a subset role.
 *
 * @see Role#isSubsetOf(Role)
 */
public class SubsetResult {
    public enum Result {
        YES, MAYBE, NO;
    }

    private final Result result;
    private final Set<Set<String>> combineDLSQueriesFromIndexPrivilegeMatchingTheseNames;

    private SubsetResult(final Result result, @Nullable final Set<Set<String>> indices) {
        this.result = Objects.requireNonNull(result, "result must be specified");
        if (indices != null) {
            this.combineDLSQueriesFromIndexPrivilegeMatchingTheseNames = Collections.unmodifiableSet(indices);
        } else {
            this.combineDLSQueriesFromIndexPrivilegeMatchingTheseNames = Collections.emptySet();
        }
    }

    public Result result() {
        return result;
    }

    public Set<Set<String>> setOfIndexNamesForCombiningDLSQueries() {
        return combineDLSQueriesFromIndexPrivilegeMatchingTheseNames;
    }

    @Override
    public int hashCode() {
        return Objects.hash(result, combineDLSQueriesFromIndexPrivilegeMatchingTheseNames);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SubsetResult other = (SubsetResult) obj;
        return result == other.result && Objects.equals(combineDLSQueriesFromIndexPrivilegeMatchingTheseNames,
                other.combineDLSQueriesFromIndexPrivilegeMatchingTheseNames);
    }

    @Override
    public String toString() {
        return "SubsetResult [result=" + result + ", combineDLSQueriesFromIndexPrivilegeMatchingTheseNames="
                + combineDLSQueriesFromIndexPrivilegeMatchingTheseNames + "]";
    }

    public static SubsetResult isASubset() {
        return new SubsetResult(Result.YES, null);
    }

    public static SubsetResult isNotASubset() {
        return new SubsetResult(Result.NO, null);
    }

    /**
     * Build MAYBE subset result
     *
     * @param indices In case the result is MAYBE, we want to know which indices
     * permissions needs modifications. So we store the index names and later
     * compare to find appropriate indices permission.
     * @return {@link SubsetResult}
     */
    public static SubsetResult mayBeASubset(final Set<Set<String>> indices) {
        return new SubsetResult(Result.MAYBE, indices);
    }

}
