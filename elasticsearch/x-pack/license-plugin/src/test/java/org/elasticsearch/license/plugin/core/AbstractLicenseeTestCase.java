/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.license.core.License;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
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
    public static void assertEmptyAck(License.OperationMode fromMode, License.OperationMode toMode, Licensee licensee) {
        License fromLicense = mock(License.class);
        when(fromLicense.operationMode()).thenReturn(fromMode);
        License toLicense = mock(License.class);
        when(toLicense.operationMode()).thenReturn(toMode);

        if (randomBoolean()) {
            fromLicense = null;
        }

        // test it
        String[] messages = licensee.acknowledgmentMessages(fromLicense, toLicense);

        assertThat(messages.length, equalTo(0));
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
    public static void assertEmptyAck(License.OperationMode fromMode, License.OperationMode toMode, Supplier<Licensee> licenseeSupplier) {
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
    public static String[] ackLicenseChange(License.OperationMode fromMode, License.OperationMode toMode, Licensee licensee) {
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
    public static String[] ackLicenseChange(License.OperationMode fromMode, License.OperationMode toMode, Supplier<Licensee> licenseeSupplier) {
        return ackLicenseChange(fromMode, toMode, licenseeSupplier.get());
    }

    /**
     * Get any {@link License.OperationMode} that have {@link License.OperationMode#allFeaturesEnabled()} all features enabled}.
     *
     * @return Never {@code null}.
     */
    public static License.OperationMode randomAllFeaturesMode() {
        return randomFrom(License.OperationMode.values(), License.OperationMode::allFeaturesEnabled);
    }

    /**
     * Get any {@link License.OperationMode} that {@link License.OperationMode#isPaid() is paid}.
     *
     * @return Never {@code null}.
     */
    public static License.OperationMode randomPaidMode() {
        return randomFrom(License.OperationMode.values(), License.OperationMode::isPaid);
    }

    /**
     * Get any {@link License.OperationMode} that {@link License.OperationMode#isPaid() is not paid}.
     *
     * @return Never {@code null}.
     */
    public static License.OperationMode randomFreeMode() {
        Predicate<License.OperationMode> isPaid = License.OperationMode::isPaid;

        return randomFrom(License.OperationMode.values(), isPaid.negate());
    }

    /**
     * Get any {@link #randomPaidMode() paid mode}, except the selected {@code mode}.
     *
     * @param mode The mode to exclude.
     * @return Never {@code null}.
     */
    public static License.OperationMode randomFromPaidExcept(License.OperationMode mode) {
        return randomValueOtherThan(mode, AbstractLicenseeTestCase::randomPaidMode);
    }

    /**
     * Get any {@link LicenseState} that {@link LicenseState#isActive() is active}.
     *
     * @return Never {@code null}.
     */
    public static LicenseState randomActiveState() {
        return randomFrom(LicenseState.values(), LicenseState::isActive);
    }

    /**
     * Get any {@link LicenseState} that {@link LicenseState#isActive() is not active}.
     *
     * @return Never {@code null}.
     */
    public static LicenseState randomInactiveState() {
        Predicate<LicenseState> isActive = LicenseState::isActive;

        return randomFrom(LicenseState.values(), isActive.negate());
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
}
