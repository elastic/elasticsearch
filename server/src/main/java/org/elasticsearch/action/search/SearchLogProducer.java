/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.action.ActionLoggerProducer;
import org.elasticsearch.index.SlowLogFields;

import java.util.Arrays;

public class SearchLogProducer implements ActionLoggerProducer<SearchLogContext> {

    public static final String LOGGER_NAME = "search.actionlog";
    public static final String[] NEVER_MATCH = new String[] { "*", "-*" };

    @Override
    public ESLogMessage produce(SearchLogContext context, SlowLogFields additionalFields) {
        ESLogMessage msg = produceCommon(context, additionalFields);
        if (isSystemUser(msg)) {
            return null;
        }
        return msg.with("query", context.getQuery()).with("indices", context.getIndices()).with("hits", context.getHits());
    }

    @Override
    public Level logLevel(SearchLogContext context, Level defaultLevel) {
        // Exclude system searches, exclude empty patterns (can we do this?)
        if (context.isSystemSearch() || Arrays.equals(NEVER_MATCH, context.getIndexNames())) {
            return Level.OFF;
        }
        return defaultLevel;
    }

    /**
     * A hacky way to determine if this is a system request.
     * Unfortunately, we do not have direct access to the user info here, and we do not even know if security is enabled at all.
     * TODO: we may want to think of a better way to do this.
     */
    private boolean isSystemUser(ESLogMessage msg) {
        String realm = msg.get("user.realm");
        String type = msg.get("auth.type");
        if (realm == null || type == null) {
            return false;
        }
        return realm.equals("__attach") && type.equals("INTERNAL");
    }
}
