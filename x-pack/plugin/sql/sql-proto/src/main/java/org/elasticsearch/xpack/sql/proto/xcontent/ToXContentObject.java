/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.xcontent;

/**
 * NB: Light-clone from XContent library to keep JDBC driver independent.
 *
 * An interface allowing to transfer an object to "XContent" using an
 * {@link XContentBuilder}. The difference between {@link ToXContentFragment}
 * and {@link ToXContentObject} is that the former outputs a fragment that
 * requires to start and end a new anonymous object externally, while the latter
 * guarantees that what gets printed out is fully valid syntax without any
 * external addition.
 */
public interface ToXContentObject extends ToXContent {

    @Override
    default boolean isFragment() {
        return false;
    }
}
