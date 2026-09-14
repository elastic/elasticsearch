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

package org.elasticsearch.gradle.internal.ospackage;

import java.io.Serializable;
import java.util.Objects;

/**
 * An explicitly registered directory entry. The Elasticsearch packaging build registers every
 * intermediate directory of the packaged files so the package managers own (and clean up) those
 * directories on uninstall.
 */
public class Directory implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String path;
    private final int permissions;
    private final boolean addParents;

    public Directory(String path, int permissions, boolean addParents) {
        this.path = path;
        this.permissions = permissions;
        this.addParents = addParents;
    }

    public String getPath() {
        return path;
    }

    public int getPermissions() {
        return permissions;
    }

    public boolean isAddParents() {
        return addParents;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof Directory == false) {
            return false;
        }
        Directory directory = (Directory) o;
        return permissions == directory.permissions && addParents == directory.addParents && Objects.equals(path, directory.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, permissions, addParents);
    }
}
