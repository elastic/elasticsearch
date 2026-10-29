/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.ValueSource;
import org.gradle.api.provider.ValueSourceParameters;

import java.nio.file.Files;
import java.nio.file.Path;

import javax.annotation.Nullable;

/**
 * Looks up the absolute path of an executable in a list of directories. Absent if none of them contains it.
 *
 * <p>Modelled as a {@link ValueSource} so that the configuration cache re-checks the host on every build instead of
 * remembering whether the tool was installed when the cache entry was stored.
 */
public abstract class PackageToolValueSource implements ValueSource<String, PackageToolValueSource.Parameters> {

    public interface Parameters extends ValueSourceParameters {
        /**
         * The name of the executable, e.g. {@code dpkg-deb}.
         */
        Property<String> getExecutable();

        /**
         * The directories to search, in order.
         */
        ListProperty<String> getSearchPath();
    }

    @Nullable
    @Override
    public String obtain() {
        String executable = getParameters().getExecutable().get();
        for (String directory : getParameters().getSearchPath().get()) {
            Path candidate = Path.of(directory, executable);
            if (Files.isRegularFile(candidate) && Files.isExecutable(candidate)) {
                return candidate.toAbsolutePath().toString();
            }
        }
        return null;
    }
}
