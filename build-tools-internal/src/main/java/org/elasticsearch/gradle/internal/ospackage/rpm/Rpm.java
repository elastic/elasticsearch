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

package org.elasticsearch.gradle.internal.ospackage.rpm;

import org.elasticsearch.gradle.internal.ospackage.PackageWriter;
import org.elasticsearch.gradle.internal.ospackage.SystemPackagingTask;
import org.gradle.api.InvalidUserDataException;
import org.gradle.work.DisableCachingByDefault;

import java.io.IOException;

/**
 * Builds an rpm package from the declared content mappings and package metadata.
 */
@DisableCachingByDefault(because = "Packaging tasks are IO bound and not worth caching")
public abstract class Rpm extends SystemPackagingTask {

    @Override
    protected PackageWriter createWriter() throws IOException {
        validate();
        return new RpmPackageWriter(this);
    }

    private void validate() {
        String packageName = getPackageName().getOrNull();
        if (packageName == null || packageName.matches("[a-zA-Z0-9-._+]+") == false) {
            throw new InvalidUserDataException(
                "Invalid package name '" + packageName + "' - a valid package name must only contain [a-zA-Z0-9-._+]"
            );
        }
        if (getVersion().isPresent() == false) {
            throw new InvalidUserDataException("RPM requires a version string");
        }
    }
}
