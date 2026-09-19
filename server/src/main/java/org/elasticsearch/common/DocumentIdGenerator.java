/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import java.util.OptionalInt;

public interface DocumentIdGenerator {

    DocumentIdGenerator DEFAULT = new DocumentIdGenerator() {
        @Override
        public String generateId() {
            return UUIDs.base64UUID();
        }

        @Override
        public String generateKOrderedId(OptionalInt routingHash) {
            return UUIDs.base64TimeBasedKOrderedUUIDWithHash(routingHash);
        }
    };

    String generateId();

    String generateKOrderedId(OptionalInt routingHash);
}
