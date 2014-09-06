/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class LdapModuleTests extends ElasticsearchTestCase {

    @Test
    public void testEnabled() throws Exception {
        assertThat(LdapModule.enabled(ImmutableSettings.EMPTY), is(false));
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc", false)
                .build();
        assertThat(LdapModule.enabled(settings), is(false));
        settings = ImmutableSettings.builder()
                .put("shield.authc.ldap.enabled", false)
                .build();
        assertThat(LdapModule.enabled(settings), is(false));
        settings = ImmutableSettings.builder()
                .put("shield.authc.ldap.enabled", true)
                .build();
        assertThat(LdapModule.enabled(settings), is(true));
    }
}
