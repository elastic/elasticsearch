/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.License.OperationMode;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Provides helper methods for {@link Licensee} implementations.
 * <p>
 * Note: This requires that this class be on the classpath for those implementations.
 */
public abstract class AbstractLicenseeTestCase extends ESTestCase {
    /**
     * Ensure when going from {@code fromMode} to {@code toMode}, nothing gets reported.
     * <p>
     * This will randomly {@code null}-out the {@code fromMode} license.
     *
     * @param fromMode Original / current license
     * @param toMode New license
     * @param licensee The licensee to test
     */
    public static void assertEmptyAck(OperationMode fromMode, OperationMode toMode, Licensee licensee) {
        License fromLicense = mock(License.class);
        when(fromLicense.operationMode()).thenReturn(fromMode);
        License toLicense = mock(License.class);
        when(toLicense.operationMode()).thenReturn(toMode);

        if (randomBoolean()) {
            fromLicense = null;
        }

        // test it
        String[] messages = licensee.acknowledgmentMessages(fromLicense, toLicense);

        assertThat(fromToMessage(fromMode, toMode), messages.length, equalTo(0));
    }

    /**
     * Ensure when going from {@code fromMode} to {@code toMode}, nothing gets reported.
     * <p>
     * This will randomly {@code null}-out the {@code fromMode} license.
     *
     * @param fromMode Original / current license
     * @param toMode New license
     * @param licenseeSupplier Supplies the licensee to test
     */
    public static void assertEmptyAck(OperationMode fromMode, OperationMode toMode, Supplier<Licensee> licenseeSupplier) {
        assertEmptyAck(fromMode, toMode, licenseeSupplier.get());
    }

    /**
     * Get the ack when changing {@code fromMode} to {@code toMode}.
     * <p>
     * This just serves to remove a lot of duplicated code.
     *
     * @param fromMode Original / current license
     * @param toMode New license
     * @param licensee The licensee to test
     */
    public static String[] ackLicenseChange(OperationMode fromMode, OperationMode toMode, Licensee licensee) {
        License fromLicense = mock(License.class);
        when(fromLicense.operationMode()).thenReturn(fromMode);
        License toLicense = mock(License.class);
        when(toLicense.operationMode()).thenReturn(toMode);

        return licensee.acknowledgmentMessages(fromLicense, toLicense);
    }

    /**
     * Ensure when going from {@code fromMode} to {@code toMode}, nothing gets reported.
     * <p>
     * This just serves to remove a lot of duplicated code.
     *
     * @param fromMode Original / current license
     * @param toMode New license
     * @param licenseeSupplier Supplies the licensee to test
     */
    public static String[] ackLicenseChange(OperationMode fromMode, OperationMode toMode, Supplier<Licensee> licenseeSupplier) {
        return ackLicenseChange(fromMode, toMode, licenseeSupplier.get());
    }

    /**
     * Randomly get {@link OperationMode#TRIAL} or {@link OperationMode#PLATINUM}.
     *
     * @return Never {@code null}.
     */
    public static OperationMode randomTrialOrPlatinumMode() {
        return randomFrom(OperationMode.TRIAL, OperationMode.PLATINUM);
    }

    /**
     * Randomly get {@link OperationMode#TRIAL}, {@link OperationMode#STANDARD}, {@link OperationMode#GOLD}, or
     * {@link OperationMode#PLATINUM}.
     *
     * @return Never {@code null}.
     */
    public static OperationMode randomTrialStandardGoldOrPlatinumMode() {
        return randomFrom(OperationMode.TRIAL, OperationMode.STANDARD, OperationMode.GOLD, OperationMode.PLATINUM);
    }

    /**
     * Randomly get any {@link OperationMode}.
     *
     * @return Never {@code null}.
     */
    public static OperationMode randomMode() {
        return randomFrom(OperationMode.values());
    }

    /**
     * Get any {@link #randomMode() mode}, except the selected {@code mode}.
     *
     * @param mode The mode to exclude.
     * @return Never {@code null}.
     */
    public static OperationMode randomModeExcept(OperationMode mode) {
        return randomValueOtherThan(mode, AbstractLicenseeTestCase::randomMode);
    }

    /**
     * Randomly get {@link LicenseState#ENABLED} or {@link LicenseState#GRACE_PERIOD}.
     *
     * @return Never {@code null}.
     */
    public static LicenseState randomEnabledOrGracePeriodState() {
        return randomFrom(LicenseState.ENABLED, LicenseState.GRACE_PERIOD);
    }

    /**
     * Get a random value from the {@code values} that passes {@code filter}.
     *
     * @param values The values to filter and randomly select from
     * @param filter The filter to apply
     * @return Never {@code null}.
     * @throws IllegalArgumentException if nothing matches the {@code filter}
     * @see #randomFrom(Object[])
     */
    public static <T> T randomFrom(T[] values, Predicate<T> filter) {
        return randomFrom(Arrays.stream(values).filter(filter).collect(Collectors.toList()));
    }

    /**
     * Get a message to show with assertions for license transition.
     *
     * @param fromMode Coming "from" mode
     * @param toMode Going "to" mode
     * @return Never {@code null}.
     */
    public static String fromToMessage(OperationMode fromMode, OperationMode toMode) {
        return String.format(Locale.ROOT, "From [%s] to [%s]", fromMode, toMode);
    }

    public static class SimpleLicenseeRegistry extends AbstractComponent implements LicenseeRegistry {
        private final List<Licensee> licensees = new ArrayList<>();
        private OperationMode operationMode;

        public SimpleLicenseeRegistry() {
            super(Settings.EMPTY);
        }

        @Override
        public void register(Licensee licensee) {
            licensees.add(licensee);
            enable();
        }

        public void enable() {
            for (Licensee licensee : licensees) {
                licensee.onChange(new Licensee.Status(operationMode, randomEnabledOrGracePeriodState()));
            }
        }

        public void disable() {
            for (Licensee licensee : licensees) {
                licensee.onChange(new Licensee.Status(operationMode, LicenseState.DISABLED));
            }
        }

        public void setOperationMode(License.OperationMode operationMode) {
            this.operationMode = operationMode;
            enable();
        }
    }
}
