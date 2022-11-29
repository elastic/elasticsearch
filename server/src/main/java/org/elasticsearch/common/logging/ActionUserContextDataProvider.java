/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.util.ContextDataProvider;
import org.apache.logging.log4j.util.StringMap;
import org.elasticsearch.action.support.user.ActionUser;
import org.elasticsearch.action.support.user.ActionUserContext;

import java.util.Map;
import java.util.Optional;

/**
 * Provides log4j "ContextData" (i.e. MDC) for the Elasticsearch {@link ActionUser}.
 * The exact MDC fields will be dependent on the implementation of the ActionUser interface.
 */
public class ActionUserContextDataProvider implements ContextDataProvider {

    @Override
    public Map<String, String> supplyContextData() {
        return getActionUser().map(this::supplyContextData).orElse(Map.of());
    }

    @Override
    public StringMap supplyStringMap() {
        return ContextDataProvider.super.supplyStringMap();
    }

    private Map<String, String> supplyContextData(ActionUser actionUser) {
        return actionUser.identifier().toEcsMap("user");
    }

    private static Optional<ActionUser> getActionUser() {
        return HeaderWarning.THREAD_CONTEXT.stream()
            .map(ActionUserContext::getEffectiveUser)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .findFirst();
    }

}
