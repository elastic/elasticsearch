/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy.entitlements;

import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.entitlement.runtime.policy.PolicyValidationException;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.Mode.READ_WRITE;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class FilesEntitlementTests extends ESTestCase {

    public void testEmptyBuild() {
        PolicyValidationException pve = expectThrows(PolicyValidationException.class, () -> FilesEntitlement.build(List.of()));
        assertEquals("must specify at least one path", pve.getMessage());
        pve = expectThrows(PolicyValidationException.class, () -> FilesEntitlement.build(null));
        assertEquals("must specify at least one path", pve.getMessage());
    }

    public void testInvalidRelativeDirectory() {
        var ex = expectThrows(
            PolicyValidationException.class,
            () -> FilesEntitlement.build(List.of((Map.of("relative_path", "foo", "mode", "read", "relative_to", "bar"))))
        );
        assertThat(ex.getMessage(), is("invalid relative directory: bar, valid values: [config, data, home]"));
    }

    public void testFileDataRelativeWithEmptyDirectory() {
        var fileData = FilesEntitlement.FileData.ofRelativePath(Path.of(""), FilesEntitlement.BaseDir.DATA, READ_WRITE);
        var dataDirs = fileData.resolvePaths(
            new PathLookup(Path.of("/home"), Path.of("/config"), new Path[] { Path.of("/data1/"), Path.of("/data2") }, Path.of("/temp"))
        );
        assertThat(dataDirs.toList(), contains(Path.of("/data1/"), Path.of("/data2")));
    }
}
