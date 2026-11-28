/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.ci;

import org.gradle.api.provider.Property;

/**
 * Per-project settings of {@link CiPartsPlugin}.
 */
public abstract class CiPartsExtension {

    /**
     * Whether the project's home part also runs {@code assemble}, which release builds verify alongside
     * {@code check}.
     */
    public abstract Property<Boolean> getIncludeAssemble();
}
