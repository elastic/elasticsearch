/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import com.github.nitram509.jmacaroons.Macaroon;
import com.github.nitram509.jmacaroons.MacaroonsBuilder;
import com.github.nitram509.jmacaroons.MacaroonsVerifier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.StringHelper;

public class MacaroonService {
    private static final Logger logger = LogManager.getLogger(MacaroonService.class);

    private final String macKey;

    public MacaroonService() {
        macKey = "key";
    }

    public String createMacaroon(String pointInTimeId) {
        logger.info("Creating macaroon for PIT [{}]", pointInTimeId);
        return new MacaroonsBuilder("", macKey, generateId()).add_first_party_caveat(pointInTimeIdCaveat(pointInTimeId))
            .getMacaroon()
            .serialize();
    }

    private String pointInTimeIdCaveat(String pointInTimeId) {
        return "pit_id = [%s]".formatted(pointInTimeId);
    }

    public boolean isMacaroonValid(String macaroonPayload, String pointInTimeId) {
        try {
            final Macaroon macaroon = MacaroonsBuilder.deserialize(macaroonPayload);
            final MacaroonsVerifier verifier = new MacaroonsVerifier(macaroon);
            verifier.satisfyExact(pointInTimeIdCaveat(pointInTimeId));
            return verifier.isValid(macKey);
        } catch (Exception e) {
            logger.warn("Failed to handle macaroon", e);
            return false;
        }
    }

    private String generateId() {
        return StringHelper.idToString(StringHelper.randomId());
    }
}
