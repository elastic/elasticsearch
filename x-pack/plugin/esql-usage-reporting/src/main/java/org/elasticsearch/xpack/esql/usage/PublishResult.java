/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import java.util.List;
import java.util.Set;

/**
 * The outcome of one {@link UsagePublisher#publish} call: the ids the Usage API accepted, and what went wrong with the others.
 */
public record PublishResult(Set<String> acceptedIds, List<Failure> failures) {

    /**
     * A failure of some of the records of a batch. {@code recordIds} empty means the whole batch.
     */
    public record Failure(Set<String> recordIds, FailureClass failureClass, String message) {}

    /**
     * What the reporter does depends on the class: network and server errors resolve by themselves, an invalid record never will,
     * and an identity or certificate problem lasts and has to be noticed.
     */
    public enum FailureClass {
        /** A network or TLS error, a 5xx, a 408 or a 429: the records stay pending and are sent again with the same ids. */
        RETRYABLE,
        /** The Usage API rejected the record itself (400, 422): it is dropped alone, with an error log, and never sent again. */
        INVALID,
        /**
         * Authentication or authorization was refused (401, 403): the records stay pending, up to the bound of the store, and an
         * alert is raised; they are dropped, with the loss logged, only when the bound is exceeded.
         */
        AUTH
    }

    public static PublishResult empty() {
        return new PublishResult(Set.of(), List.of());
    }
}
