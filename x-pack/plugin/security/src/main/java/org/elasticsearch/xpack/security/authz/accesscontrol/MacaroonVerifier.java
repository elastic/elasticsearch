/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.accesscontrol;

import com.github.nitram509.jmacaroons.GeneralSecurityRuntimeException;
import com.github.nitram509.jmacaroons.Macaroon;
import com.github.nitram509.jmacaroons.MacaroonsVerifier;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.transport.TransportRequest;

public final class MacaroonVerifier {

    private final Macaroon macaroon;
    private final byte[] key;

    public MacaroonVerifier(Macaroon macaroon, byte[] key) {
        this.macaroon = macaroon;
        this.key = key;
    }

    public boolean verify(TransportRequest request) throws GeneralSecurityRuntimeException {
        final MacaroonsVerifier macaroonsVerifier = new MacaroonsVerifier(macaroon);
        if (request instanceof SearchRequest searchRequest && searchRequest.pointInTimeBuilder() != null) {
            macaroonsVerifier.satisfyExact("restrict access to PIT " + searchRequest.pointInTimeBuilder().getEncodedId());
        }
        return macaroonsVerifier.isValid(key);
    }
}
