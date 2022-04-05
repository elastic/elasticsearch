/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import com.github.nitram509.jmacaroons.MacaroonsBuilder;

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
        return new MacaroonsBuilder(location, macKey, generateId()).add_first_party_caveat(createIndexNamesCaveat(indexNames))
            .add_first_party_caveat("pid_id = [%s]".formatted(pointInTimeId))
            .getMacaroon()
            .serialize();
    }

    private String createIndexNamesCaveat(Collection<String> indexNames) {
        return "index_names = [%s]".formatted(Strings.collectionToCommaDelimitedString(indexNames));
    }

    private String generateId() {
        // quick and dirty
        return StringHelper.idToString(StringHelper.randomId());
    }
}
