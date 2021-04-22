/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.common.io.stream.Writeable;

/**
 * An interface for status response of the stored or running async search
 */
public interface SearchStatusResponse extends Writeable {

    /**
     * Returns a timestamp when the search will be expired, in milliseconds since epoch.
     */
    long getExpirationTime();

    /**
     * Sets the new expiration time, in milliseconds since epoch.
     */
    void setExpirationTime(long expirationTime);
}
