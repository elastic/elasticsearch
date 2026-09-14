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

import org.elasticsearch.gradle.internal.ospackage.SystemPackagingTask;
import org.gradle.api.Project;
import org.gradle.api.internal.file.copy.CopyAction;
import org.gradle.work.DisableCachingByDefault;
import org.redline_rpm.header.Architecture;
import org.redline_rpm.header.Os;
import org.redline_rpm.header.RpmType;

import java.util.List;

/**
 * Builds an rpm package from the configured copy specs and package metadata.
 */
@DisableCachingByDefault(because = "Packaging tasks are IO bound and not worth caching")
public abstract class Rpm extends SystemPackagingTask {

    public Rpm() {
        super();
        getArchiveExtension().set("rpm");
    }

    @Override
    protected String assembleArchiveName() {
        StringBuilder name = new StringBuilder(getPackageName());
        if (getVersion() != null) {
            name.append('-').append(getVersion());
        }
        if (getRelease() != null && getRelease().isEmpty() == false) {
            name.append('-').append(getRelease());
        }
        if (getArchString() != null) {
            name.append('.').append(getArchString());
        }
        String extension = getArchiveExtension().getOrNull();
        if (extension != null) {
            name.append('.').append(extension);
        }
        return name.toString();
    }

    @Override
    protected CopyAction createCopyAction() {
        return new RpmCopyAction(this);
    }

    @Override
    public void applyConventions(Project project) {
        super.applyConventions(project);
        getExten().getAddParentDirs().convention(parentOrValue(exten -> exten.getAddParentDirs(), true));
        getExten().getArchStr().convention(parentOrValue(exten -> exten.getArchStr(), Architecture.NOARCH.name()));
        getExten().getOs().convention(parentOrValue(exten -> exten.getOs(), Os.UNKNOWN));
        getExten().getType().convention(parentOrValue(exten -> exten.getType(), RpmType.BINARY));
        getExten().getPrefixes().convention(parentExtenPrefixes());
    }

    private org.gradle.api.provider.Provider<List<String>> parentExtenPrefixes() {
        return getParentExten() != null ? getParentExten().getPrefixes().orElse(List.of()) : getProviderFactory().provider(List::of);
    }
}
