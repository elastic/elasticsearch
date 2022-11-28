/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.user;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Optional;

public class ActionUserContext {

    public interface Resolver {
        Optional<ActionUser> resolve(ThreadContext context);
    };

    private static final SetOnce<Resolver> RESOLVER = new SetOnce<>();

    /**
     * Retrieves the current effective user for the given thread context.
     * The "effective user" is the user on whose behalf (that is, with their identity and permissions)
     * the current action is executing. It may not be the user who is "authenticated" (and there may
     * not be an authenticated user - for example this may be a background action executing as an
     * internal system user).
     */
    public static Optional<ActionUser> getEffectiveUser(ThreadContext threadContext) {
        Resolver r = RESOLVER.get();
        if (r == null) {
            return Optional.empty();
        }
        return r.resolve(threadContext);
    }

    public static void setActionUserResolver(Resolver resolver) {
        RESOLVER.set(resolver);
    }

}
