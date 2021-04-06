/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * Encapsulates license checking for autoscaling.
 */
public class AutoscalingLicenseChecker {

    private final BooleanSupplier isAutoscalingAllowed;

    /**
     * Constructs an autoscaling license checker with the default rule based on the license state for checking if autoscaling is allowed.
     */
    AutoscalingLicenseChecker() {
        this(() -> XPackPlugin.getSharedLicenseState().checkFeature(XPackLicenseState.Feature.AUTOSCALING));
    }

    /**
     * Constructs an autoscaling license checker with the specified boolean supplier.
     *
     * @param isAutoscalingAllowed a boolean supplier that should return true is autoscaling is allowed and otherwise false.
     */
    public AutoscalingLicenseChecker(final BooleanSupplier isAutoscalingAllowed) {
        this.isAutoscalingAllowed = Objects.requireNonNull(isAutoscalingAllowed);
    }

    /**
     * Returns whether or not autoscaling is allowed.
     *
     * @return true if autoscaling is allowed, otherwise false
     */
    public boolean isAutoscalingAllowed() {
        return isAutoscalingAllowed.getAsBoolean();
    }

}
