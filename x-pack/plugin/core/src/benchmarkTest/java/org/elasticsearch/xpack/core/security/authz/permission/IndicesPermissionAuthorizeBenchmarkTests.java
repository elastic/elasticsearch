/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Pins what {@link IndicesPermissionAuthorizeBenchmark} exercises, so a change that alters the shape of the
 * work - most importantly one that stops backing indices from sharing an {@code IndexAccessControl} - fails
 * here instead of silently shifting the numbers.
 */
public class IndicesPermissionAuthorizeBenchmarkTests extends ESTestCase {

    public void testDataStreamGrantsEveryBackingIndexAndSharesOneIndexAccessControl() {
        final IndicesPermissionAuthorizeBenchmark bench = newBenchmark(randomIntBetween(2, 50));

        final IndicesAccessControl.IndexAccessControl probe = bench.authorizeDataStream();
        assertThat(probe, is(notNullValue()));
        assertRestrictionsMatchParams(bench.dlsFls, probe);

        // Sharing is a property of one IndicesAccessControl: the cache lives for a single authorize() call,
        // so the reference must come from the same instance as the entries compared against it.
        final IndicesAccessControl iac = bench.authorizeDataStreamAccessControl();
        assertThat(iac.isGranted(), is(true));
        final List<String> backingIndexNames = bench.backingIndexNames();
        final IndicesAccessControl.IndexAccessControl shared = iac.getIndexPermissions(backingIndexNames.get(0));
        assertThat(shared, is(notNullValue()));
        for (String backingIndex : backingIndexNames) {
            assertSame(
                "backing index [" + backingIndex + "] must share one IndexAccessControl",
                shared,
                iac.getIndexPermissions(backingIndex)
            );
        }
    }

    public void testDirectBackingIndexIsGrantedWithSameRestrictions() {
        final IndicesPermissionAuthorizeBenchmark bench = newBenchmark(randomIntBetween(1, 10));

        final IndicesAccessControl.IndexAccessControl direct = bench.authorizeBackingIndexDirectly();
        assertThat(direct, is(notNullValue()));
        assertRestrictionsMatchParams(bench.dlsFls, direct);
    }

    private static IndicesPermissionAuthorizeBenchmark newBenchmark(int backingIndices) {
        final IndicesPermissionAuthorizeBenchmark bench = new IndicesPermissionAuthorizeBenchmark();
        bench.backingIndices = backingIndices;
        bench.dlsFls = randomFrom(IndicesPermissionAuthorizeBenchmark.DlsFls.values());
        bench.matchingGroups = randomIntBetween(1, 5);
        bench.setup();
        return bench;
    }

    private static void assertRestrictionsMatchParams(
        IndicesPermissionAuthorizeBenchmark.DlsFls dlsFls,
        IndicesAccessControl.IndexAccessControl access
    ) {
        final boolean expectFls = dlsFls == IndicesPermissionAuthorizeBenchmark.DlsFls.FLS
            || dlsFls == IndicesPermissionAuthorizeBenchmark.DlsFls.BOTH;
        final boolean expectDls = dlsFls == IndicesPermissionAuthorizeBenchmark.DlsFls.DLS
            || dlsFls == IndicesPermissionAuthorizeBenchmark.DlsFls.BOTH;
        assertThat("FLS for " + dlsFls, access.getFieldPermissions().hasFieldLevelSecurity(), is(expectFls));
        assertThat("DLS for " + dlsFls, access.getDocumentPermissions().hasDocumentLevelPermissions(), is(expectDls));
        // Groups are declared in the role, never contributed by an ImplicitPrivilegesProvider.
        assertThat(access.isDlsFlsImplicit(), is(false));
    }
}
