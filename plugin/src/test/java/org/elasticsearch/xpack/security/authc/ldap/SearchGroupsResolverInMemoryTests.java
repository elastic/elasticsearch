/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import java.util.List;
import java.util.concurrent.ExecutionException;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPURL;
import com.unboundid.ldap.sdk.ResultCode;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapTestCase;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SearchGroupsResolverInMemoryTests extends LdapTestCase {

    /**
     * Tests that a client-side timeout in the asynchronous LDAP SDK is treated as a failure, rather
     * than simply returning no results.
     */
    public void testSearchTimeoutIsFailure() throws Exception {

        ldapServers[0].setProcessingDelayMillis(100);

        final LDAPConnectionOptions options = new LDAPConnectionOptions();
        options.setConnectTimeoutMillis(500);
        options.setResponseTimeoutMillis(5);

        final LDAPURL ldapurl = new LDAPURL(ldapUrls()[0]);
        final LDAPConnection connection = LdapUtils.privilegedConnect(
                () -> new LDAPConnection(options,
                        ldapurl.getHost(), ldapurl.getPort())
        );

        final Settings settings = Settings.builder()
                .put("group_search.base_dn", "ou=groups,o=sevenSeas")
                .put("group_search.scope", LdapSearchScope.SUB_TREE)
                .build();
        final SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        final PlainActionFuture<List<String>> future = new PlainActionFuture<>();
        resolver.resolve(connection,
                "cn=William Bush,ou=people,o=sevenSeas",
                TimeValue.timeValueSeconds(30),
                logger,
                null, future);

        final ExecutionException exception = expectThrows(ExecutionException.class, future::get);
        final Throwable cause = exception.getCause();
        assertThat(cause, instanceOf(LDAPException.class));
        assertThat(((LDAPException) cause).getResultCode(), is(ResultCode.TIMEOUT));
    }

}