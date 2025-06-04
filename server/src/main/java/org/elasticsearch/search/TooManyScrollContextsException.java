/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class TooManyScrollContextsException extends ElasticsearchException {

    public TooManyScrollContextsException(int maxOpenScrollCount, String maxOpenScrollSettingName) {
        super(
            "Trying to create too many scroll contexts. Must be less than or equal to: ["
                + maxOpenScrollCount
                + "]. "
                + "This limit can be set by changing the ["
                + maxOpenScrollSettingName
                + "] setting."
        );
    }

    public TooManyScrollContextsException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.TOO_MANY_REQUESTS;
    }
}
