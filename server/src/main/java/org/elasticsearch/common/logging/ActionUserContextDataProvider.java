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
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Provides log4j "ContextData" (i.e. MDC) for the Elasticsearch {@link ActionUser}.
 * The exact MDC fields will be dependent on the implementation of the ActionUser interface.
 */
public class ActionUserContextDataProvider implements ContextDataProvider {

    /**
     * This is set once by the {@code Node} constructor, but it uses {@link CopyOnWriteArraySet} to ensure that tests can run in parallel.
     * <p>
     * Integration tests will create separate nodes within the same classloader, thus leading to a shared, {@code static} state.
     * In order for all tests to appropriately be handled, this must be able to remember <em>all</em> {@link ActionUserContext}s that it is
     * given in a thread safe manner.
     * <p>
     * For actual usage, multiple nodes do not share the same JVM and therefore this will only be set once in practice.
     */
    static final CopyOnWriteArraySet<ActionUserContext> USER_CONTEXT = new CopyOnWriteArraySet<>();

    /**
     * Set the {@link ThreadContext} used to add warning headers to network responses.
     * <p>
     * This is expected to <em>only</em> be invoked by the {@code Node}'s constructor (therefore once outside of tests).
     *
     * @param actionUserContext The {@code ActionUserContext} that is active within this {@code Node})
     * @throws IllegalStateException if this {@code threadContext} has already been set
     */
    public static void setContext(ActionUserContext actionUserContext) {
        Objects.requireNonNull(actionUserContext, "Cannot register a null ActionUserContext");

        // add returning false means it _did_ have it already
        if (USER_CONTEXT.add(actionUserContext) == false) {
            throw new IllegalStateException("Double-setting ActionUserContext not allowed!");
        }
    }

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
        return USER_CONTEXT.stream().map(ActionUserContext::getEffectiveUser).filter(Optional::isPresent).map(Optional::get).findFirst();
    }

}
