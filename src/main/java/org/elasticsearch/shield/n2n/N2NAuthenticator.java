/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.n2n;

import org.elasticsearch.common.Nullable;

import java.net.InetAddress;
import java.security.Principal;

/**
 *
 */
public interface N2NAuthenticator {

    N2NAuthenticator NO_AUTH = new N2NAuthenticator() {
        @Override
        public boolean authenticate(@Nullable Principal peerPrincipal, InetAddress peerAddress, int peerPort) {
            return true;
        }
    };

    boolean authenticate(@Nullable Principal peerPrincipal, InetAddress peerAddress, int peerPort);


    class Compound implements N2NAuthenticator {

        private N2NAuthenticator[] authenticators;

        public Compound(N2NAuthenticator... authenticators) {
            this.authenticators = authenticators;
        }

        @Override
        public boolean authenticate(@Nullable Principal peerPrincipal, InetAddress peerAddress, int peerPort) {
            for (int i = 0; i < authenticators.length; i++) {
                if (!authenticators[i].authenticate(peerPrincipal, peerAddress, peerPort)) {
                    return false;
                }
            }
            return true;
        }
    }
}
