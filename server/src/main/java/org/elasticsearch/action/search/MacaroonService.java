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
import org.elasticsearch.common.Strings;

import java.util.Collection;

public class MacaroonService {
    private static final Logger logger = LogManager.getLogger(MacaroonService.class);

    private final String macKey;
    private final String location;

    public MacaroonService() {
        // TODO from settings
        macKey = "key";
        location = "elasticsearch";
    }

    public String createMacaroon(Collection<String> indexNames, String pointInTimeId) {
        logger.info("Creating macaroon for [{}] and [{}]", indexNames, pointInTimeId);
        return new MacaroonsBuilder(location, macKey, generateId()).add_first_party_caveat(indexNamesCaveat(indexNames))
            .add_first_party_caveat(pointInTimeIdCaveat(pointInTimeId))
            .getMacaroon()
            .serialize();
    }

    private String pointInTimeIdCaveat(String pointInTimeId) {
        return "pid_id = [%s]".formatted(pointInTimeId);
    }

    public boolean isMacaroonValid(String macaroonPayload, String pointInTimeId) {
        Macaroon macaroon = MacaroonsBuilder.deserialize(macaroonPayload);
        MacaroonsVerifier verifier = new MacaroonsVerifier(macaroon);
        verifier.satisfyExact(pointInTimeIdCaveat(pointInTimeId));
        verifier.satisfyGeneral(this::satisfyIndexNamesCaveat);
        return verifier.isValid(macKey);
    }

    private boolean satisfyIndexNamesCaveat(String caveat) {
        // this is bad -> nothing prevents users from adding more index_names caveats
        return caveat.startsWith("index_names = ");
    }

    private String indexNamesCaveat(Collection<String> indexNames) {
        return "index_names = [%s]".formatted(Strings.collectionToCommaDelimitedString(indexNames));
    }

    private String generateId() {
        // quick and dirty
        return StringHelper.idToString(StringHelper.randomId());
    }
}
