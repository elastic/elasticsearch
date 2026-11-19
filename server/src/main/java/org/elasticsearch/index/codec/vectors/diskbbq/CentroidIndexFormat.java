/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

public enum CentroidIndexFormat {
    /**
     * A flat list of centroids, possibly with a second layer of children
     */
    FLAT(0);

    private final int id;

    CentroidIndexFormat(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public static CentroidIndexFormat fromId(int id) {
        for (CentroidIndexFormat format : values()) {
            if (format.id == id) {
                return format;
            }
        }
        throw new IllegalArgumentException("Unknown CentroidIndexFormat id: " + id);
    }
}
