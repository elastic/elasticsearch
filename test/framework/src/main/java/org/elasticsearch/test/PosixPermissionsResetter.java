/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test;

import org.junit.Assert;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;

/** Stores the posix attributes for a path and resets them on close. */
public class PosixPermissionsResetter implements AutoCloseable {
    private final PosixFileAttributeView attributeView;
    private final Set<PosixFilePermission> permissions;
    public PosixPermissionsResetter(Path path) throws IOException {
        attributeView = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        Assert.assertNotNull(attributeView);
        permissions = attributeView.readAttributes().permissions();
    }
    @Override
    public void close() throws IOException {
        attributeView.setPermissions(permissions);
    }
    public void setPermissions(Set<PosixFilePermission> newPermissions) throws IOException {
        attributeView.setPermissions(newPermissions);
    }

    public Set<PosixFilePermission> getCopyPermissions() {
        return new HashSet<>(permissions);
    }
}
