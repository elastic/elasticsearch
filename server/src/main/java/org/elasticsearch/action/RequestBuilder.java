/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;

public interface RequestBuilder<Request, Response extends RefCounted> {
    /**
     * This method returns the request that this builder builds. Depending on the implementation, it might return a new request with each
     * call or the same request with each call.
     */
    Request request();

    ActionFuture<Response> execute();

    Response get();

    Response get(TimeValue timeout);

    void execute(ActionListener<Response> listener);
}
