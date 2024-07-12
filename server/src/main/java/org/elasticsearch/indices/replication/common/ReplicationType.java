/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.replication.common;

public enum ReplicationType {

    DOCUMENT,
    SEGMENT;

    public static ReplicationType parseString(String replicationType) {
        try {
            return ReplicationType.valueOf(replicationType);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Could not parse ReplicationStrategy for [" + replicationType + "]");
        } catch (NullPointerException npe) {
            // return a default value for null input
            return DOCUMENT;
        }
    }
}
