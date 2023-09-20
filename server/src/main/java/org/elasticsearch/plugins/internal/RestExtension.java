/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.action.cat.AbstractCatAction;

import java.util.function.Predicate;

public interface RestExtension {
    /**
     * Returns a filter that determines which cat actions are exposed in /_cat.
     *
     * The filter should return {@code true} if an action should be included,
     * or {@code false} otherwise.
     */
    Predicate<AbstractCatAction> getCatActionsFilter();

    /**
     * Returns a filter that determines which rest actions are exposed.
     *
     * The filter should return {@code false} if an action should be included,
     * or {@code false} if the paths
     * @return
     */
    Predicate<RestHandler> getActionsFilter();

    /**
     * Returns a rest extension which allows all rest endpoints through.
     */
    static RestExtension allowAll() {
        return new RestExtension() {
            @Override
            public Predicate<AbstractCatAction> getCatActionsFilter() {
                return action -> true;
            }

            @Override
            public Predicate<RestHandler> getActionsFilter() {
                return handler -> true;
            }
        };
    }
}
