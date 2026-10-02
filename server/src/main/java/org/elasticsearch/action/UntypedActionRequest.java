/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * An action request with an unspecified response type.
 * <p>
 * In due course we intend to make {@link ActionRequest} strongly-typed in its {@link ActionResponse} type, and then to migrate each
 * {@link UntypedActionRequest} instances to the new typed world. Until the strongly-typed {@link ActionRequest} is available, please
 * continue to use {@link UntypedActionRequest} for all new request types.
 */
public abstract class UntypedActionRequest extends ActionRequest {
    public UntypedActionRequest() {
        super();
    }

    public UntypedActionRequest(StreamInput in) throws IOException {
        super(in);
    }
}
