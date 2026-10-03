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

import java.io.File;
import java.io.IOException;

/**
 * Receives the package entries produced by {@link SystemPackagingTask} and writes them into a
 * package-format specific archive (redline for rpm, jdeb for deb).
 */
public interface PackageWriter {

    /** Adds a regular file entry. {@code fileTypeFlags} carries rpm {@code RPMFILE_*} bits, 0 for none. */
    void addFile(String path, File source, int mode, String user, String group, int fileTypeFlags) throws IOException;

    /** Adds a package-owned directory entry. */
    void addDirectory(String path, int mode, String user, String group) throws IOException;

    /** Adds a symbolic link entry. */
    void addLink(String path, String target) throws IOException;

    /** Writes the package archive. */
    void finish() throws IOException;
}
