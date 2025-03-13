/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class CcrLicenseCheckerTests extends ESTestCase {

    public void testNoAuthenticationInfo() {
        final boolean isCcrAllowed = randomBoolean();
        final CcrLicenseChecker checker = new CcrLicenseChecker(() -> isCcrAllowed, () -> true) {

            @Override
            User getUser(final ThreadContext threadContext) {
                return null;
            }

        };
        final AtomicBoolean invoked = new AtomicBoolean();
        checker.hasPrivilegesToFollowIndices(
            new ThreadContext(Settings.EMPTY),
            mock(RemoteClusterClient.class),
            new String[] { randomAlphaOfLength(8) },
            e -> {
                invoked.set(true);
                assertThat(e, instanceOf(IllegalStateException.class));
                assertThat(e, hasToString(containsString("missing or unable to read authentication info on request")));
            }
        );
        assertTrue(invoked.get());
    }

}
