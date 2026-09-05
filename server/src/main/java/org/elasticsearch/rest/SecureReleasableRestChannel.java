/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.common.settings.SecureReleasable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class SecureReleasableRestChannel extends DelegatingRestChannel {

    private final ThreadContext threadContext;

    public SecureReleasableRestChannel(RestChannel delegate, ThreadContext threadContext) {
        super(delegate);
        this.threadContext = threadContext;
    }

    @Override
    public void sendResponse(RestResponse response) {
        Releasable toRelease = collectSecureReleasableTransients(threadContext);
        if (toRelease != null) {
            response.addReleasable(toRelease);
        }
        super.sendResponse(response);
    }

    @Nullable
    static Releasable collectSecureReleasableTransients(ThreadContext threadContext) {
        if (threadContext.getTransientHeaders().isEmpty()) {
            return null;
        }
        List<Releasable> collected = new ArrayList<>();
        for (Object value : threadContext.getTransientHeaders().values()) {
            collectSecureReleasables(value, collected);
        }
        return collected.isEmpty() ? null : Releasables.wrap(collected);
    }

    private static void collectSecureReleasables(Object value, List<Releasable> releasables) {
        if (value instanceof SecureReleasable secureReleasable) {
            releasables.add(secureReleasable);
        } else if (value instanceof Map<?, ?> map) {
            for (Object nested : map.values()) {
                if (nested instanceof SecureReleasable secureReleasable) {
                    releasables.add(secureReleasable);
                }
            }
        }
    }
}
