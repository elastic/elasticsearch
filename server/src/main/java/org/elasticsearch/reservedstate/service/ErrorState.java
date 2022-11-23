/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;

import java.util.List;

import static org.elasticsearch.ExceptionsHelper.stackTrace;

record ErrorState(String namespace, Long version, List<String> errors, ReservedStateErrorMetadata.ErrorKind errorKind) {
    ErrorState(String namespace, Long version, Exception e, ReservedStateErrorMetadata.ErrorKind errorKind) {
        this(namespace, version, List.of(stackTrace(e)), errorKind);
    }

    public String toString() {
        return String.join(", ", errors());
    }
}
