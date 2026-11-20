/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.IndexAuthorizationResult;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl.IndexAccessControl;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class AuthorizationEngineTests extends ESTestCase {

    public void testIndexAuthorizationResultFailureMessage() {
        // Note: The "." below is a literal dot (not a regex) this matches index names starting with a period.
        final RestrictedIndices restrictedIndex = new RestrictedIndices(Automatons.patterns(".*"));
        assertThat(
            AuthorizationEngine.IndexAuthorizationResult.getFailureDescription(List.of("index-1", "index-2", ".index-3"), restrictedIndex),
            is("on indices [index-1,index-2] and restricted indices [.index-3]")
        );

        assertThat(
            AuthorizationEngine.IndexAuthorizationResult.getFailureDescription(List.of("index-1"), restrictedIndex),
            is("on indices [index-1]")
        );

        assertThat(
            AuthorizationEngine.IndexAuthorizationResult.getFailureDescription(
                List.of(".index-1", ".index-2", ".index-3"),
                restrictedIndex
            ),
            is("on restricted indices [.index-1,.index-2,.index-3]")
        );
    }

    public void testIndexAuthorizationResultGrantedHasNoFailureContext() {
        final String index = randomIndexName();
        assertThat(failureContext(true, Set.of(index), Set.of(index), index), nullValue());
    }

    public void testIndexAuthorizationResultFailureContextForTimestampExclusion() {
        final String excluded = randomIndexName();
        final String failureContext = failureContext(false, Set.of(excluded), Set.of(excluded), excluded);
        assertThat(failureContext, containsString(TIMESTAMP_EXCLUSION));
        assertThat(failureContext, not(containsString(excluded)));
    }

    public void testIndexAuthorizationResultReportsPrivilegeDenialBeforeTimestampExclusion() {
        final String excluded = randomIndexName();
        final String denied = randomValueOtherThan(excluded, ESTestCase::randomIndexName);
        final String[] requested = randomBoolean() ? new String[] { excluded, denied } : new String[] { denied, excluded };
        assertThat(failureContext(false, Set.of(excluded), Set.of(excluded), requested), is("on indices [" + denied + "]"));
    }

    public void testIndexAuthorizationResultFailureContextWithoutRequestedIndices() {
        final String excluded = randomIndexName();
        assertThat(failureContext(false, Set.of(excluded), Set.of(excluded)), containsString(TIMESTAMP_EXCLUSION));
        assertThat(failureContext(false, Set.of(), Set.of()), nullValue());
    }

    private static final String TIMESTAMP_EXCLUSION = "because field level security excludes [@timestamp]";

    private static String failureContext(
        boolean granted,
        Set<String> permittedIndices,
        Set<String> timestampExcludedIndices,
        String... requestedIndices
    ) {
        final Map<String, IndexAccessControl> indexPermissions = new HashMap<>();
        permittedIndices.forEach(index -> indexPermissions.put(index, IndexAccessControl.ALLOW_ALL));
        final IndicesAccessControl accessControl = new IndicesAccessControl(
            granted,
            () -> indexPermissions,
            () -> timestampExcludedIndices
        );
        final RequestInfo requestInfo = new RequestInfo(
            AuthenticationTestHelper.builder().build(),
            new SearchRequest(requestedIndices),
            TransportSearchAction.TYPE.name(),
            null
        );
        return new IndexAuthorizationResult(accessControl).getFailureContext(requestInfo, new RestrictedIndices(Automatons.patterns(".*")));
    }

}
