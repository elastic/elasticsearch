/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermissionAuthorizeBenchmark.DlsFls;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Pins what {@link LimitedRoleAuthorizeBenchmark} exercises: the owner-limited-by-key composition grants every backing
 * index of the data stream, each composed entry carries FLS or DLS exactly when either side does, and all of them share one
 * composed {@code IndexAccessControl}.
 */
public class LimitedRoleAuthorizeBenchmarkTests extends ESTestCase {

    public void testLimitedDataStreamGrantsEveryBackingIndexWithComposedRestrictions() {
        final LimitedRoleAuthorizeBenchmark bench = newBenchmark(randomIntBetween(2, 50));

        final IndicesAccessControl iac = bench.authorizeDataStreamLimitedAccessControl();
        assertThat(iac.isGranted(), is(true));
        for (String backingIndex : bench.backingIndexNames()) {
            assertComposed(bench, iac.getIndexPermissions(backingIndex));
        }
        assertComposed(bench, bench.authorizeDataStreamLimited());

        // The composition is memoized on the identity of its inputs, which are themselves shared per data stream, so
        // the data stream name and every backing index share one composed IndexAccessControl.
        final IndicesAccessControl.IndexAccessControl composed = iac.getIndexPermissions(LimitedRoleAuthorizeBenchmark.DATA_STREAM);
        assertThat(composed, is(notNullValue()));
        for (String backingIndex : bench.backingIndexNames()) {
            assertSame(
                "backing index [" + backingIndex + "] must share the composed IndexAccessControl",
                composed,
                iac.getIndexPermissions(backingIndex)
            );
        }
    }

    public void testLimitedDirectBackingIndexCarriesComposedRestrictions() {
        final LimitedRoleAuthorizeBenchmark bench = newBenchmark(randomIntBetween(1, 10));
        assertComposed(bench, bench.authorizeBackingIndexDirectlyLimited());
    }

    public void testOwnerOnlyReferenceMatchesOwnerRestrictions() {
        final LimitedRoleAuthorizeBenchmark bench = newBenchmark(randomIntBetween(1, 10));
        final IndicesAccessControl.IndexAccessControl access = bench.authorizeDataStreamOwnerOnly();
        assertThat(access, is(notNullValue()));
        assertThat(access.getFieldPermissions().hasFieldLevelSecurity(), is(hasFls(bench.ownerDlsFls)));
        assertThat(access.getDocumentPermissions().hasDocumentLevelPermissions(), is(hasDls(bench.ownerDlsFls)));
    }

    private static LimitedRoleAuthorizeBenchmark newBenchmark(int backingIndices) {
        final LimitedRoleAuthorizeBenchmark bench = new LimitedRoleAuthorizeBenchmark();
        bench.backingIndices = backingIndices;
        bench.ownerDlsFls = randomFrom(DlsFls.values());
        bench.keyDlsFls = randomFrom(DlsFls.values());
        bench.setup();
        return bench;
    }

    private static void assertComposed(LimitedRoleAuthorizeBenchmark bench, IndicesAccessControl.IndexAccessControl access) {
        assertThat(access, is(notNullValue()));
        final String shape = "owner=" + bench.ownerDlsFls + " key=" + bench.keyDlsFls;
        assertThat(
            "FLS for " + shape,
            access.getFieldPermissions().hasFieldLevelSecurity(),
            is(hasFls(bench.ownerDlsFls) || hasFls(bench.keyDlsFls))
        );
        assertThat(
            "DLS for " + shape,
            access.getDocumentPermissions().hasDocumentLevelPermissions(),
            is(hasDls(bench.ownerDlsFls) || hasDls(bench.keyDlsFls))
        );
        // Both roles are declared, never contributed by an ImplicitPrivilegesProvider.
        assertThat(access.isDlsFlsImplicit(), is(false));
    }

    private static boolean hasFls(DlsFls dlsFls) {
        return dlsFls == DlsFls.FLS || dlsFls == DlsFls.BOTH;
    }

    private static boolean hasDls(DlsFls dlsFls) {
        return dlsFls == DlsFls.DLS || dlsFls == DlsFls.BOTH;
    }
}
