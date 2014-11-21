/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.n2n;

import org.elasticsearch.common.Nullable;

import java.net.InetAddress;
import java.security.Principal;

/**
 *
 */
public interface N2NAuthenticator {

    boolean authenticate(@Nullable Principal peerPrincipal, @Nullable String profile, InetAddress peerAddress, int peerPort);

}
