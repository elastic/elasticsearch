/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.junit.Before;

public class RealmConfigBootstrapCheckTests extends ESTestCase {
    private Settings.Builder builder;

    @Before
    public void setup() {
        builder = Settings.builder();
        builder.put("path.home", createTempDir());
    }

    public void testRealmsNoErrors() throws Exception {
        withRealm(PkiRealmSettings.TYPE, randomInt(5), builder);
        withRealm(KerberosRealmSettings.TYPE, 1, builder);
        withRealm(FileRealmSettings.TYPE, 1, builder);
        withRealm(NativeRealmSettings.TYPE, 1, builder);
        assertTrue(runCheck(builder.build()).isSuccess());
    }

    public void testRealmsWithMultipleInternalRealmsConfiguredFailingBootstrapCheck() throws Exception {
        withRealm(PkiRealmSettings.TYPE, 2, builder);
        withRealm(KerberosRealmSettings.TYPE, 1, builder);
        withRealm(FileRealmSettings.TYPE, 2, builder);
        withRealm(NativeRealmSettings.TYPE, 1, builder);
        BootstrapCheck.BootstrapCheckResult result = runCheck(builder.build());
        assertTrue(result.isFailure());
        assertEquals("multiple [" + FileRealmSettings.TYPE + "] realms are configured. [" + FileRealmSettings.TYPE
                + "] is an internal realm and therefore there can only be one such realm configured", result.getMessage());
    }

    public void testRealmsWithMultipleKerberosRealmsConfiguredFailingBootstrapCheck() throws Exception {
        withRealm(PkiRealmSettings.TYPE, 2, builder);
        withRealm(KerberosRealmSettings.TYPE, 2, builder);
        withRealm(FileRealmSettings.TYPE, 1, builder);
        withRealm(NativeRealmSettings.TYPE, 1, builder);
        BootstrapCheck.BootstrapCheckResult result = runCheck(builder.build());
        assertTrue(result.isFailure());
        assertEquals("multiple [" + KerberosRealmSettings.TYPE + "] realms are configured. For [" + KerberosRealmSettings.TYPE
                + "] there can only be one such realm configured", result.getMessage());
    }

    public void testRealmsWithEmptyTypeConfiguredFailingBootstrapCheck() throws Exception {
        withRealm(PkiRealmSettings.TYPE, 2, builder);
        withRealm(KerberosRealmSettings.TYPE, 1, builder);
        withRealm(FileRealmSettings.TYPE, 1, builder);
        withRealm(NativeRealmSettings.TYPE, 1, builder);
        String name = randomAlphaOfLength(4) ;
        builder.put("xpack.security.authc.realms."+name+".type", "");
        BootstrapCheck.BootstrapCheckResult result = runCheck(builder.build());
        assertTrue(result.isFailure());
        assertEquals("missing realm type for [" + name + "] realm", result.getMessage());
    }

    private static void withRealm(String type, int instances, Settings.Builder builder) {
        for (int i = 0 ; i < instances ; i++) {
            builder.put("xpack.security.authc.realms."+randomAlphaOfLength(4) +".type", type);
        }
    }

    private BootstrapCheck.BootstrapCheckResult runCheck(Settings settings) throws Exception {
        return new RealmConfigBootstrapCheck().check(new BootstrapContext(settings, null));
    }
}
