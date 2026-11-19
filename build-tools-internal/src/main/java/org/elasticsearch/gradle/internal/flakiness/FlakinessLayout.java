/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

/** Shared filesystem layout for the handoff between per-project resolution and repo-wide scanning. */
public final class FlakinessLayout {

    /**
     * Directory relative to the settings root where each project writes its uniquely named targets file.
     * Keeping the files together lets the scan discover them with a flat glob rather than walking project
     * build directories. Keep this value in sync with {@code FLAKINESS_TARGETS_DIR} in {@code domain.ts}.
     */
    public static final String TARGETS_DIR = "build/flakiness/project-targets";

    private FlakinessLayout() {}
}
