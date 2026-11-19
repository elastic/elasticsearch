/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.esql.QueryMetricsListener;
import org.elasticsearch.xpack.core.ssl.SslProfile;
import org.elasticsearch.xpack.core.ssl.extension.SslProfileExtension;
import org.junit.After;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ServiceLoader;
import java.util.Set;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class UsageSslProfileTests extends ESTestCase {

    @After
    public void resetProfile() {
        // The holder is static: do not leak the profile stored by one test into the others of the same JVM.
        new UsageSslProfile().applyProfile("xpack.esql.usage_reporting.ssl", null);
    }

    public void testPrefixEndsInSsl() {
        Set<String> prefixes = new UsageSslProfile().getSettingPrefixes();
        assertThat(prefixes, equalTo(Set.of("xpack.esql.usage_reporting.ssl")));
        prefixes.forEach(p -> assertThat(p, endsWith(".ssl")));
    }

    public void testApplyProfileStoresIt() {
        // SslProfile is an interface of a dozen methods backed by real key material; the holder only stores the reference.
        SslProfile profile = mock(SslProfile.class);
        new UsageSslProfile().applyProfile("xpack.esql.usage_reporting.ssl", profile);
        assertThat(UsageSslProfile.current(), sameInstance(profile));
    }

    /** A typo in a service file would otherwise be found only when a node starts. */
    public void testServiceFilesResolve() throws IOException {
        assertThat(ServiceLoader.load(SslProfileExtension.class).stream().anyMatch(p -> p.type() == UsageSslProfile.class), equalTo(true));
        assertThat(UsageListener.class.getConstructors().length, equalTo(1));
        assertThat(UsageListener.class.getConstructors()[0].getParameterTypes()[0], equalTo(EsqlUsageReportingPlugin.class));
        // Not through ServiceLoader: it requires a public no-argument constructor, while the extension loader passes the plugin.
        String resource = "META-INF/services/" + QueryMetricsListener.class.getName();
        try (InputStream in = UsageListener.class.getClassLoader().getResourceAsStream(resource)) {
            assertThat(in != null, equalTo(true));
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8).strip(), equalTo(UsageListener.class.getName()));
        }
    }
}
