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

import org.elasticsearch.gradle.internal.ospackage.SystemPackagingTask;
import org.gradle.api.Project;
import org.gradle.api.internal.file.copy.CopyAction;
import org.gradle.work.DisableCachingByDefault;

import java.io.File;

/**
 * Builds a deb package from the configured copy specs and package metadata.
 */
@DisableCachingByDefault(because = "Packaging tasks are IO bound and not worth caching")
public abstract class Deb extends SystemPackagingTask {

    public Deb() {
        super();
        getArchiveExtension().set("deb");
    }

    @Override
    protected String assembleArchiveName() {
        StringBuilder name = new StringBuilder(getPackageName());
        if (getVersion() != null) {
            name.append('_').append(getVersion());
        }
        if (getRelease() != null && getRelease().isEmpty() == false) {
            name.append('-').append(getRelease());
        }
        if (getArchString() != null) {
            name.append('_').append(getArchString());
        }
        String extension = getArchiveExtension().getOrNull();
        if (extension != null) {
            name.append('.').append(extension);
        }
        return name.toString();
    }

    @Override
    protected CopyAction createCopyAction() {
        // use the task-private temporary dir so parallel deb tasks of the same project cannot
        // overwrite each other's control files
        return new DebCopyAction(this, new File(getTemporaryDir(), "debian"));
    }

    @Override
    public void applyConventions(Project project) {
        super.applyConventions(project);
        getExten().getUid().convention(parentOrValue(exten -> exten.getUid(), 0));
        getExten().getGid().convention(parentOrValue(exten -> exten.getGid(), 0));
        getExten().getPackageGroup().convention(parentOrValue(exten -> exten.getPackageGroup(), "java"));
        getExten().getArchStr().convention(parentOrValue(exten -> exten.getArchStr(), "all"));
        getExten().getMaintainer().convention(parentOrValue(exten -> exten.getMaintainer(), System.getProperty("user.name", "")));
        getExten().getUploaders().convention(parentOrValue(exten -> exten.getUploaders(), ""));
    }
}
