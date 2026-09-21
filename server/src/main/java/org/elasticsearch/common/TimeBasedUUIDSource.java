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
import java.util.function.Supplier;

class TimeBasedUUIDSource implements UUIDSource {

    private final TimeBasedUUIDGenerator timeBasedGenerator;
    private final TimeBasedKOrderedUUIDGenerator kOrderedGenerator;

    TimeBasedUUIDSource(Supplier<Long> timestampSupplier, Supplier<Integer> sequenceIdSupplier, Supplier<byte[]> macAddressSupplier) {
        this.timeBasedGenerator = new TimeBasedUUIDGenerator(timestampSupplier, sequenceIdSupplier, macAddressSupplier);
        this.kOrderedGenerator = new TimeBasedKOrderedUUIDGenerator(timestampSupplier, sequenceIdSupplier, macAddressSupplier);
    }

    @Override
    public String base64UUID() {
        return timeBasedGenerator.getBase64UUID();
    }

    @Override
    public String base64TimeBasedKOrderedUUIDWithHash(OptionalInt hash) {
        return kOrderedGenerator.getBase64UUID(hash);
    }
}
