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
import com.github.nitram509.jmacaroons.verifier.TimestampCaveatVerifier;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.transport.TransportRequest;

import static org.elasticsearch.xpack.security.authz.accesscontrol.MacaroonUtils.PIT_CAVEAT_KEY;
import static org.elasticsearch.xpack.security.authz.accesscontrol.MacaroonUtils.READ_ONLY_CAVEAT_KEY;

public final class MacaroonVerifier {

    private final Macaroon macaroon;
    private final byte[] key;

    public MacaroonVerifier(Macaroon macaroon, byte[] key) {
        this.macaroon = macaroon;
        this.key = key;
    }

    public boolean verify(String action, TransportRequest request) throws GeneralSecurityRuntimeException {
        MacaroonsVerifier macaroonsVerifier = new MacaroonsVerifier(macaroon);
        macaroonsVerifier.satisfyGeneral(new TimestampCaveatVerifier());
        if (action.startsWith("indices:data/read")) {
            macaroonsVerifier.satisfyExact(READ_ONLY_CAVEAT_KEY);
        }
        if (request instanceof SearchRequest searchRequest && searchRequest.pointInTimeBuilder() != null) {
            macaroonsVerifier.satisfyExact(PIT_CAVEAT_KEY + " = " + searchRequest.pointInTimeBuilder().getEncodedId());
        }
        return macaroonsVerifier.isValid(key);
    }
}
