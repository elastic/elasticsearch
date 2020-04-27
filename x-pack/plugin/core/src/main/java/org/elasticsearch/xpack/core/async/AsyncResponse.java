/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.common.io.stream.Writeable;

public interface AsyncResponse<T extends AsyncResponse<?>> extends Writeable {
    /**
     * When this response will expire as a timestamp in milliseconds since epoch.
     */
    long getExpirationTime();

    /**
     * Returns a copy of this object with a new expiration time
     */
    T withExpirationTime(long expirationTimeMillis);

}
