/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class RealmConfigTests extends ESTestCase {

    private RealmConfig.RealmIdentifier realmIdentifier;
    private Settings globalSettings;
    private Environment environment;
    private ThreadContext threadContext;

    @Before
    public void setUp() throws Exception {
        realmIdentifier = new RealmConfig.RealmIdentifier(randomAlphaOfLengthBetween(4, 12), randomAlphaOfLengthBetween(4, 12));
        environment = Mockito.mock(Environment.class);
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        threadContext = new ThreadContext(globalSettings);
        super.setUp();
    }

    public void testWillPassWhenOrderSettingIsConfigured() {
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(RealmSettings.realmSettingPrefix(realmIdentifier) + "order", 0)
            .build();

        RealmConfig realmConfig = new RealmConfig(realmIdentifier, settings, environment, threadContext);
        assertEquals(0, realmConfig.order);
    }

    public void testWillFailWhenOrderSettingIsMissing() {
        Settings settings = Settings.builder().put(globalSettings).build();
        var e = expectThrows(IllegalArgumentException.class, () -> new RealmConfig(realmIdentifier, settings, environment, threadContext));
        assertThat(e.getMessage(), containsString("'order' is a mandatory parameter for realm config"));
    }

    public void testWillNotFailWhenOrderIsMissingAndDisabled() {
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ENABLED_SETTING), false)
            .build();
        final RealmConfig realmConfig = new RealmConfig(realmIdentifier, settings, environment, threadContext);
        assertThat(realmConfig.enabled(), is(false));
    }
}
