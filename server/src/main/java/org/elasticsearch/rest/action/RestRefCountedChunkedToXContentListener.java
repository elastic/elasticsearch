/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.xcontent.ToXContent;

/**
 * Same as {@link RestChunkedToXContentListener} but decrements the ref count on the response it receives by one after serialization of the
 * response.
 */
public class RestRefCountedChunkedToXContentListener<Response extends ChunkedToXContent & RefCounted> extends RestChunkedToXContentListener<
    Response> {
    public RestRefCountedChunkedToXContentListener(RestChannel channel) {
        super(channel);
    }

    public RestRefCountedChunkedToXContentListener(RestChannel channel, ToXContent.Params params) {
        super(channel, params);
    }

    @Override
    protected Releasable releasableFromResponse(Response response) {
        response.mustIncRef();
        return Releasables.assertOnce(response::decRef);
    }
}
