/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.rest.action.cat.AbstractCatAction;

import java.util.ServiceLoader;
import java.util.function.Predicate;
import java.util.function.Supplier;

public interface RestExtension {
    /**
     * Returns a filter that determines which cat actions are exposed in /_cat.
     *
     * The filter should return {@code true} if an action should be included,
     * or {@code false} otherwise.
     */
    Predicate<AbstractCatAction> getCatActionsFilter();

    static RestExtension load(Supplier<RestExtension> fallback) {
        var loader = ServiceLoader.load(RestExtension.class);
        var extensions = loader.stream().toList();
        if (extensions.size() > 1) {
            throw new IllegalStateException("More than one rest extension found");
        } else if (extensions.size() == 0) {
            return fallback.get();
        }
        return extensions.get(0).get();
    }
}
