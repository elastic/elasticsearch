/*
 * @notice
 * Copyright 2011-2019 the original author or authors.
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is derived from the nebula gradle-ospackage-plugin
 * (https://github.com/nebula-plugins/gradle-ospackage-plugin), reduced to the
 * functionality used by the Elasticsearch build and rewritten in Java.
 */

package org.elasticsearch.gradle.internal.ospackage.deb;

import org.elasticsearch.gradle.internal.ospackage.PackageWriter;
import org.elasticsearch.gradle.internal.ospackage.SystemPackagingTask;
import org.gradle.api.InvalidUserDataException;
import org.gradle.work.DisableCachingByDefault;

import java.io.File;
import java.io.IOException;

/**
 * Builds a deb package from the declared content mappings and package metadata.
 */
@DisableCachingByDefault(because = "Packaging tasks are IO bound and not worth caching")
public abstract class Deb extends SystemPackagingTask {

    @Override
    protected PackageWriter createWriter() throws IOException {
        validate();
        // use the task-private temporary dir so parallel deb tasks of the same project cannot
        // overwrite each other's control files
        return new DebPackageWriter(this, new File(getTemporaryDir(), "debian"));
    }

    private void validate() {
        String version = getVersion().getOrNull();
        if (version == null
            || version.isEmpty()
            || Character.isDigit(version.charAt(0)) == false
            || version.matches("[A-Za-z0-9.+:~-]+") == false) {
            throw new InvalidUserDataException(
                "Invalid upstream version '" + version + "' - a valid version must start with a digit and only contain [A-Za-z0-9.+:~-]"
            );
        }
        String packageName = getPackageName().getOrNull();
        if (packageName == null
            || packageName.length() < 2
            || Character.isLetterOrDigit(packageName.charAt(0)) == false
            || packageName.matches("[a-z0-9.+-]+") == false) {
            throw new InvalidUserDataException(
                "Invalid package name '"
                    + packageName
                    + "' - a valid package name must start with an alphanumeric character, have a length of at least two"
                    + " characters and only contain [a-z0-9.+-]"
            );
        }
    }
}
