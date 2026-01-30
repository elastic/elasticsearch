/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import org.elasticsearch.entitlement.rules.EntitlementRule;
import org.elasticsearch.entitlement.rules.EntitlementRules;
import org.elasticsearch.entitlement.rules.Policies;

import java.nio.charset.spi.CharsetProvider;
import java.text.spi.BreakIteratorProvider;
import java.text.spi.CollatorProvider;
import java.text.spi.DateFormatProvider;
import java.text.spi.DateFormatSymbolsProvider;
import java.text.spi.DecimalFormatSymbolsProvider;
import java.text.spi.NumberFormatProvider;
import java.util.Locale;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.logging.LogManager;
import java.util.spi.CalendarDataProvider;
import java.util.spi.CalendarNameProvider;
import java.util.spi.CurrencyNameProvider;
import java.util.spi.LocaleNameProvider;
import java.util.spi.LocaleServiceProvider;
import java.util.spi.TimeZoneNameProvider;

public class L10nInstrumentation implements InstrumentationConfig {
    @Override
    public void init(Consumer<EntitlementRule> addRule) {
        EntitlementRules.on(addRule, LogManager.class).protectedCtor().enforce(Policies::changeJvmGlobalState).elseThrowNotEntitled();

        EntitlementRules.on(addRule, CalendarDataProvider.class)
            .protectedCtor()
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, CalendarNameProvider.class)
            .protectedCtor()
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, CurrencyNameProvider.class)
            .protectedCtor()
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, LocaleNameProvider.class)
            .protectedCtor()
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, LocaleServiceProvider.class)
            .protectedCtor()
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, TimeZoneNameProvider.class)
            .protectedCtor()
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, CharsetProvider.class).protectedCtor().enforce(Policies::changeJvmGlobalState).elseThrowNotEntitled();

        EntitlementRules.on(addRule, TimeZone.class)
            .callingVoidStatic(TimeZone::setDefault, TimeZone.class)
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, BreakIteratorProvider.class)
            .protectedCtor()
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, CollatorProvider.class).protectedCtor().enforce(Policies::changeJvmGlobalState).elseThrowNotEntitled();

        EntitlementRules.on(addRule, DateFormatProvider.class)
            .protectedCtor()
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, DateFormatSymbolsProvider.class)
            .protectedCtor()
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, DecimalFormatSymbolsProvider.class)
            .protectedCtor()
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, NumberFormatProvider.class)
            .protectedCtor()
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, Locale.class)
            .callingVoidStatic(Locale::setDefault, Locale.Category.class, Locale.class)
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled()
            .callingVoidStatic(Locale::setDefault, Locale.class)
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();
    }
}
