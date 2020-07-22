/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

/**
 * Interface for classes that help encode double-valued spatial coordinates x/y to
 * their integer-encoded serialized form and decode them back
 */
interface CoordinateEncoder {
    int encodeX(double x);
    int encodeY(double y);
    double decodeX(int x);
    double decodeY(int y);
}
