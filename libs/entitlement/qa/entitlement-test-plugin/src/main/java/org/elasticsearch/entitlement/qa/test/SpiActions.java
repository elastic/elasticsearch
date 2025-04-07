/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.spi.SelectorProvider;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.ALWAYS_DENIED;

@SuppressWarnings({ "unused" /* called via reflection */ })
class SpiActions {
    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createBreakIteratorProvider() {
        new DummyImplementations.DummyBreakIteratorProvider();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createCollatorProvider() {
        new DummyImplementations.DummyCollatorProvider();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createDateFormatProvider() {
        new DummyImplementations.DummyDateFormatProvider();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createDateFormatSymbolsProvider() {
        new DummyImplementations.DummyDateFormatSymbolsProvider();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createDecimalFormatSymbolsProvider() {
        new DummyImplementations.DummyDecimalFormatSymbolsProvider();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createNumberFormatProvider() {
        new DummyImplementations.DummyNumberFormatProvider();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createCalendarDataProvider() {
        new DummyImplementations.DummyCalendarDataProvider();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createCalendarNameProvider() {
        new DummyImplementations.DummyCalendarNameProvider();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createCurrencyNameProvider() {
        new DummyImplementations.DummyCurrencyNameProvider();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createLocaleNameProvider() {
        new DummyImplementations.DummyLocaleNameProvider();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createTimeZoneNameProvider() {
        new DummyImplementations.DummyTimeZoneNameProvider();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createLocaleServiceProvider() {
        new DummyImplementations.DummyLocaleServiceProvider();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void getInheritedChannel() throws IOException {
        try (Channel channel = SelectorProvider.provider().inheritedChannel()) {}
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createSelectorProvider() {
        new DummyImplementations.DummySelectorProvider();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createAsynchronousChannelProvider() {
        new DummyImplementations.DummyAsynchronousChannelProvider();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createCharsetProvider() {
        new DummyImplementations.DummyCharsetProvider();
    }

    private SpiActions() {}
}
