/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.entitlement.runtime.policy.FileUtils.isAbsolutePath;
import static org.hamcrest.Matchers.is;

public class FileUtilsTests extends ESTestCase {

    public void testPathIsAbsolute() {
        var windowsNamedPipe = "\\\\.\\pipe";
        var windowsDosAbsolutePath = "C:\\temp";
        var unixAbsolutePath = "/tmp/foo";
        var unixStyleUncPath = "//C/temp";
        var uncPath = "\\\\C\\temp";
        var longPath = "\\\\?\\C:\\temp";

        var relativePath = "foo";
        var headingSlashRelativePath = "\\foo";

        assertThat(isAbsolutePath(windowsNamedPipe), is(true));
        assertThat(isAbsolutePath(windowsDosAbsolutePath), is(true));
        assertThat(isAbsolutePath(unixAbsolutePath), is(true));
        assertThat(isAbsolutePath(unixStyleUncPath), is(true));
        assertThat(isAbsolutePath(uncPath), is(true));
        assertThat(isAbsolutePath(longPath), is(true));

        assertThat(isAbsolutePath(relativePath), is(false));
        assertThat(isAbsolutePath(headingSlashRelativePath), is(false));
        assertThat(isAbsolutePath(""), is(false));
    }
}
