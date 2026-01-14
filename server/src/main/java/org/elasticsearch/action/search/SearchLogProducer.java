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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.action.ActionLoggerProducer;
import org.elasticsearch.index.SlowLogFields;

public class SearchLogProducer implements ActionLoggerProducer<SearchLogContext> {

    private final NamedWriteableRegistry namedWriteableRegistry;

    SearchLogProducer(NamedWriteableRegistry namedWriteableRegistry) {
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    public ESLogMessage produce(SearchLogContext context, SlowLogFields additionalFields) {
        ESLogMessage msg = produceCommon(context, additionalFields);
        if (isSystemUser(msg)) {
            return null;
        }
        return msg.with("query", context.getQuery())
            .with("indices", context.getIndices(namedWriteableRegistry))
            .with("hits", context.getHits());
    }

    @Override
    public Level logLevel(SearchLogContext context, Level defaultLevel) {
        if (isSystemSearch(context)) {
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

    /**
     * Is this a system search which should not be logged?
     */
    private boolean isSystemSearch(SearchLogContext context) {
        String opaqueId = context.getOpaqueId();
        // Kibana task manager queries
        if (opaqueId != null && opaqueId.contains("kibana:task%20manager:run")) {
            return true;
        }
        return false;
    }
}
