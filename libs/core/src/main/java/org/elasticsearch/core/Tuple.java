/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

public record Tuple<V1, V2>(V1 v1, V2 v2) {

    public static <V1, V2> Tuple<V1, V2> tuple(V1 v1, V2 v2) {
        return new Tuple<>(v1, v2);
    }
}
