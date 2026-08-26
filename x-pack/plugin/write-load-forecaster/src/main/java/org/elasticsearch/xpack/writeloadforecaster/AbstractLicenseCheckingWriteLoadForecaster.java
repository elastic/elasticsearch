/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadforecaster;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.OptionalDouble;
import java.util.function.BooleanSupplier;

public abstract class AbstractLicenseCheckingWriteLoadForecaster implements WriteLoadForecaster {
    private static final Logger logger = LogManager.getLogger(AbstractLicenseCheckingWriteLoadForecaster.class);

    private final BooleanSupplier hasValidLicenseSupplier;

    @SuppressWarnings("unused") // modified via VH_HAS_VALID_LICENSE_FIELD
    protected volatile boolean hasValidLicense;

    public AbstractLicenseCheckingWriteLoadForecaster(BooleanSupplier licenseSupplier) {
        this.hasValidLicenseSupplier = licenseSupplier;
    }

    /**
     * Used to atomically {@code getAndSet()} the {@link #hasValidLicense} field. This is better than an
     * {@link java.util.concurrent.atomic.AtomicBoolean} because it takes one less pointer dereference on each read.
     */
    private static final VarHandle VH_HAS_VALID_LICENSE_FIELD;

    static {
        try {
            VH_HAS_VALID_LICENSE_FIELD = MethodHandles.lookup()
                .in(AbstractLicenseCheckingWriteLoadForecaster.class)
                .findVarHandle(AbstractLicenseCheckingWriteLoadForecaster.class, "hasValidLicense", boolean.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public abstract ProjectMetadata.Builder withWriteLoadForecastForWriteIndex(String dataStreamName, ProjectMetadata.Builder metadata);

    public abstract OptionalDouble getForecastedWriteLoad(IndexMetadata indexMetadata);

    @Override
    public final void refreshLicense() {
        final var newValue = hasValidLicenseSupplier.getAsBoolean();
        final var oldValue = (boolean) VH_HAS_VALID_LICENSE_FIELD.getAndSet(this, newValue);
        if (newValue != oldValue) {
            logger.info("license state changed, now [{}]", newValue ? "valid" : "not valid");
        }
    }
}
