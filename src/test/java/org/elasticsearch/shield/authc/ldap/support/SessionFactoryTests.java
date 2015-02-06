/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap.support;

import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.util.ssl.HostNameSSLSocketVerifier;
import com.unboundid.util.ssl.TrustAllSSLSocketVerifier;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

public class SessionFactoryTests extends ElasticsearchTestCase {

    @Test
    public void connectionFactoryReturnsCorrectLDAPConnectionOptionsWithDefaultSettings() {
        SessionFactory factory = createSessionFactory();
        LDAPConnectionOptions options = factory.connectionOptions(ImmutableSettings.EMPTY);
        assertThat(options.followReferrals(), is(equalTo(true)));
        assertThat(options.allowConcurrentSocketFactoryUse(), is(equalTo(true)));
        assertThat(options.getConnectTimeoutMillis(), is(equalTo(5000)));
        assertThat(options.getResponseTimeoutMillis(), is(equalTo(5000L)));
        assertThat(options.getSSLSocketVerifier(), is(instanceOf(HostNameSSLSocketVerifier.class)));
    }

    @Test
    public void connectionFactoryReturnsCorrectLDAPConnectionOptions() {
        Settings settings = settingsBuilder()
                .put(SessionFactory.TIMEOUT_TCP_CONNECTION_SETTING, "10ms")
                .put(SessionFactory.HOSTNAME_VERIFICATION_SETTING, "false")
                .put(SessionFactory.TIMEOUT_TCP_READ_SETTING, "20ms")
                .put(SessionFactory.FOLLOW_REFERRALS_SETTING, "false")
                .build();
        SessionFactory factory = createSessionFactory();
        LDAPConnectionOptions options = factory.connectionOptions(settings);
        assertThat(options.followReferrals(), is(equalTo(false)));
        assertThat(options.allowConcurrentSocketFactoryUse(), is(equalTo(true)));
        assertThat(options.getConnectTimeoutMillis(), is(equalTo(10)));
        assertThat(options.getResponseTimeoutMillis(), is(equalTo(20L)));
        assertThat(options.getSSLSocketVerifier(), is(instanceOf(TrustAllSSLSocketVerifier.class)));
    }

    private SessionFactory createSessionFactory() {
        return new SessionFactory(new RealmConfig("_name")) {

            @Override
            public LdapSession open(String user, SecuredString password) {
                return null;
            }
        };
    }
}
