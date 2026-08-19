/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.aws;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
* Lazy supplier for an identifier such as a region, bucket, or client name. We cannot use randomness like
* {@link ESTestCase#randomIdentifier()} when creating the test fixtures in the first place because this happens in static context, so
* instead we create one of these and defer the creation of the identifier itself until the test actually starts running.
*/
public class DynamicIdentifierSupplier implements Supplier<String> {
    private final AtomicReference<String> generatedIdentifier = new AtomicReference<>();
    private final Supplier<String> prefixSupplier;

    public DynamicIdentifierSupplier(Supplier<String> prefixSupplier) {
        this.prefixSupplier = prefixSupplier;
    }

    @Override
    public String get() {
        return Objects.requireNonNullElseGet(generatedIdentifier.get(), this::generateAndGet);
    }

    private String generateAndGet() {
        final var newRegion = ESTestCase.randomIdentifier(prefixSupplier.get());
        return Objects.requireNonNullElse(generatedIdentifier.compareAndExchange(null, newRegion), newRegion);
    }

    public static Supplier<String> testClassIdentifierSupplier(String prefixPart) {
        return new DynamicIdentifierSupplier(
            () -> LuceneTestCase.getTestClass().getSimpleName().toLowerCase(Locale.ROOT) + "-" + prefixPart + "-"
        );
    }
}
