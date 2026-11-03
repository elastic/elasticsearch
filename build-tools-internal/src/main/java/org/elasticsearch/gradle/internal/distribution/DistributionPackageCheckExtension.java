/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;

/**
 * Configures which package {@link InternalDistributionPackageCheckPlugin} checks. The package is passed in rather
 * than looked up so that the plugin does not need to reach into the project building it.
 */
public abstract class DistributionPackageCheckExtension {

    /**
     * The deb or rpm package to check. Its producing task is run before the checks.
     */
    public abstract RegularFileProperty getPackageFile();

    public abstract Property<PackageMetadata.Type> getPackageType();

    /**
     * The directories searched for {@code dpkg-deb} or {@code rpm}. The checks are skipped, and not run as part of
     * {@code check}, if the tool for the package type is not found.
     */
    public abstract ListProperty<String> getToolSearchPath();
}
