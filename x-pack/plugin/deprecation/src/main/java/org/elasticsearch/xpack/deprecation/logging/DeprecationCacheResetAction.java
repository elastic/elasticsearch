/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation.logging;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
    Resets deprecation indexing rate limiting cache on each node.
 */
public class DeprecationCacheResetAction extends ActionType<ActionResponse.Empty> {
    public static final DeprecationCacheResetAction INSTANCE = new DeprecationCacheResetAction();
    public static final String NAME = "cluster:admin/deprecation/cache/reset";

    private DeprecationCacheResetAction() {
        super(NAME, in -> ActionResponse.Empty.INSTANCE);
    }

    public static class Request extends ActionRequest {
        public Request() {
            super();
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

}
