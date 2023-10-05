/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.features;

import java.util.Arrays;

/**
 * Represents the major version era a feature was introduced
 */
public enum FeatureEra {
    V_6(6, false),
    V_7(7, true),
    V_8(8, true);

    private static final int FIRST_PUBLISHABLE_ERA = Arrays.stream(values()).filter(FeatureEra::isPublishable).findFirst().get().era();

    /**
     * {@code true} if the {@code era} is one that is published by this node
     */
    public static boolean isPublishable(int era) {
        return era >= FIRST_PUBLISHABLE_ERA;
    }

    private final int era;
    private final boolean publishable;

    FeatureEra(int era, boolean publishable) {
        this.era = era;
        this.publishable = publishable;
    }

    public int era() {
        return era;
    }

    public boolean isPublishable() {
        return publishable;
    }
}
