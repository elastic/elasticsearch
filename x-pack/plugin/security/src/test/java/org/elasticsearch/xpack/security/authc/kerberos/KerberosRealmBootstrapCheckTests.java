/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.kerberos;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class KerberosRealmBootstrapCheckTests extends ESTestCase {

    public void testBootstrapCheckFailsForMultipleKerberosRealms() throws IOException {
        final Path tempDir = createTempDir();
        final Settings settings1 = buildKerberosRealmSettings("kerb1", false, tempDir);
        final Settings settings2 = buildKerberosRealmSettings("kerb2", false, tempDir);
        final Settings settings3 = realm("pki1", PkiRealmSettings.TYPE, Settings.builder()).build();
        final Settings settings =
                Settings.builder().put("path.home", tempDir).put(settings1).put(settings2).put(settings3).build();
        final BootstrapContext context = new BootstrapContext(settings, null);
        final KerberosRealmBootstrapCheck kerbRealmBootstrapCheck =
                new KerberosRealmBootstrapCheck(TestEnvironment.newEnvironment(settings));
        final BootstrapCheck.BootstrapCheckResult result = kerbRealmBootstrapCheck.check(context);
        assertThat(result, is(notNullValue()));
        assertThat(result.isFailure(), is(true));
        assertThat(result.getMessage(), equalTo("multiple [" + KerberosRealmSettings.TYPE + "] realms are configured. ["
                + KerberosRealmSettings.TYPE + "] can only have one such realm configured"));
    }

    public void testBootstrapCheckFailsForMissingKeytabFile() throws IOException {
        final Path tempDir = createTempDir();
        final Settings settings =
                Settings.builder().put("path.home", tempDir).put(buildKerberosRealmSettings("kerb1", true, tempDir)).build();
        final BootstrapContext context = new BootstrapContext(settings, null);
        final KerberosRealmBootstrapCheck kerbRealmBootstrapCheck =
                new KerberosRealmBootstrapCheck(TestEnvironment.newEnvironment(settings));
        final BootstrapCheck.BootstrapCheckResult result = kerbRealmBootstrapCheck.check(context);
        assertThat(result, is(notNullValue()));
        assertThat(result.isFailure(), is(true));
        assertThat(result.getMessage(),
                equalTo("configured service key tab file [" + tempDir.resolve("kerb1.keytab").toString() + "] does not exist"));
    }

    public void testBootstrapCheckFailsForMissingRealmType() throws IOException {
        final Path tempDir = createTempDir();
        final String name = "kerb1";
        final Settings settings1 = buildKerberosRealmSettings("kerb1", false, tempDir);
        final Settings settings2 = realm(name, randomFrom("", "    "), Settings.builder()).build();
        final Settings settings =
                Settings.builder().put("path.home", tempDir).put(settings1).put(settings2).build();
        final BootstrapContext context = new BootstrapContext(settings, null);
        final KerberosRealmBootstrapCheck kerbRealmBootstrapCheck =
                new KerberosRealmBootstrapCheck(TestEnvironment.newEnvironment(settings));
        final BootstrapCheck.BootstrapCheckResult result = kerbRealmBootstrapCheck.check(context);
        assertThat(result, is(notNullValue()));
        assertThat(result.isFailure(), is(true));
        assertThat(result.getMessage(), equalTo("missing realm type for [" + name + "] realm"));
    }

    public void testBootstrapCheckSucceedsForCorrectConfiguration() throws IOException {
        final Path tempDir = createTempDir();
        final Settings finalSettings =
                Settings.builder().put("path.home", tempDir).put(buildKerberosRealmSettings("kerb1", false, tempDir)).build();
        final BootstrapContext context = new BootstrapContext(finalSettings, null);
        final KerberosRealmBootstrapCheck kerbRealmBootstrapCheck =
                new KerberosRealmBootstrapCheck(TestEnvironment.newEnvironment(finalSettings));
        final BootstrapCheck.BootstrapCheckResult result = kerbRealmBootstrapCheck.check(context);
        assertThat(result, is(notNullValue()));
        assertThat(result.isSuccess(), is(true));
    }

    public void testBootstrapCheckSucceedsForNoKerberosRealms() throws IOException {
        final Path tempDir = createTempDir();
        final Settings finalSettings = Settings.builder().put("path.home", tempDir).build();
        final BootstrapContext context = new BootstrapContext(finalSettings, null);
        final KerberosRealmBootstrapCheck kerbRealmBootstrapCheck =
                new KerberosRealmBootstrapCheck(TestEnvironment.newEnvironment(finalSettings));
        final BootstrapCheck.BootstrapCheckResult result = kerbRealmBootstrapCheck.check(context);
        assertThat(result, is(notNullValue()));
        assertThat(result.isSuccess(), is(true));
    }

    private Settings buildKerberosRealmSettings(final String name, final boolean missingKeytab, final Path tempDir) throws IOException {
        final Settings.Builder builder = Settings.builder();
        if (missingKeytab == false) {
            KerberosTestCase.writeKeyTab(tempDir.resolve(name + ".keytab"), null);
        }
        builder.put(KerberosTestCase.buildKerberosRealmSettings(tempDir.resolve(name + ".keytab").toString()));
        return realm(name, KerberosRealmSettings.TYPE, builder).build();
    }

    private Settings.Builder realm(final String name, final String type, final Settings.Builder settings) {
        final String prefix = RealmSettings.PREFIX + name + ".";
        if (type != null) {
            settings.put("type", type);
        }
        final Settings.Builder builder = Settings.builder().put(settings.normalizePrefix(prefix).build(), false);
        return builder;
    }
}
