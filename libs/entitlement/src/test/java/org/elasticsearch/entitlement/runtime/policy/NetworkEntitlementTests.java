/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class NetworkEntitlementTests extends ESTestCase {

    public void testMatchesActions() {
        var listenEntitlement = new NetworkEntitlement(List.of(NetworkEntitlement.LISTEN));
        var emptyEntitlement = new NetworkEntitlement(List.of());
        var connectAcceptEntitlement = new NetworkEntitlement(List.of(NetworkEntitlement.CONNECT, NetworkEntitlement.ACCEPT));

        assertThat(listenEntitlement.matchActions(0), is(true));
        assertThat(listenEntitlement.matchActions(NetworkEntitlement.LISTEN_ACTION), is(true));
        assertThat(listenEntitlement.matchActions(NetworkEntitlement.ACCEPT_ACTION), is(false));
        assertThat(listenEntitlement.matchActions(NetworkEntitlement.CONNECT_ACTION), is(false));
        assertThat(listenEntitlement.matchActions(NetworkEntitlement.LISTEN_ACTION | NetworkEntitlement.ACCEPT_ACTION), is(false));
        assertThat(listenEntitlement.matchActions(NetworkEntitlement.LISTEN_ACTION | NetworkEntitlement.CONNECT_ACTION), is(false));
        assertThat(listenEntitlement.matchActions(NetworkEntitlement.CONNECT_ACTION | NetworkEntitlement.ACCEPT_ACTION), is(false));

        assertThat(connectAcceptEntitlement.matchActions(0), is(true));
        assertThat(connectAcceptEntitlement.matchActions(NetworkEntitlement.LISTEN_ACTION), is(false));
        assertThat(connectAcceptEntitlement.matchActions(NetworkEntitlement.ACCEPT_ACTION), is(true));
        assertThat(connectAcceptEntitlement.matchActions(NetworkEntitlement.CONNECT_ACTION), is(true));
        assertThat(connectAcceptEntitlement.matchActions(NetworkEntitlement.LISTEN_ACTION | NetworkEntitlement.ACCEPT_ACTION), is(false));
        assertThat(connectAcceptEntitlement.matchActions(NetworkEntitlement.LISTEN_ACTION | NetworkEntitlement.CONNECT_ACTION), is(false));
        assertThat(connectAcceptEntitlement.matchActions(NetworkEntitlement.CONNECT_ACTION | NetworkEntitlement.ACCEPT_ACTION), is(true));

        assertThat(emptyEntitlement.matchActions(0), is(true));
        assertThat(emptyEntitlement.matchActions(NetworkEntitlement.LISTEN_ACTION), is(false));
        assertThat(emptyEntitlement.matchActions(NetworkEntitlement.ACCEPT_ACTION), is(false));
        assertThat(emptyEntitlement.matchActions(NetworkEntitlement.CONNECT_ACTION), is(false));
        assertThat(emptyEntitlement.matchActions(NetworkEntitlement.LISTEN_ACTION | NetworkEntitlement.ACCEPT_ACTION), is(false));
        assertThat(emptyEntitlement.matchActions(NetworkEntitlement.LISTEN_ACTION | NetworkEntitlement.CONNECT_ACTION), is(false));
        assertThat(emptyEntitlement.matchActions(NetworkEntitlement.CONNECT_ACTION | NetworkEntitlement.ACCEPT_ACTION), is(false));
    }
}
