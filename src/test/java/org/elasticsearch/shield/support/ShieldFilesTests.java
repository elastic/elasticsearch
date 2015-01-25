/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import com.google.common.collect.Sets;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Locale;
import java.util.Set;

import static java.nio.file.attribute.PosixFilePermission.*;
import static org.elasticsearch.shield.support.ShieldFiles.openAtomicMoveWriter;
import static org.hamcrest.Matchers.is;

public class ShieldFilesTests extends ElasticsearchTestCase {

    @Test
    public void testThatOriginalPermissionsAreKept() throws Exception {
        Path path = newTempFile().toPath();
        Files.write(path, "foo".getBytes(Charsets.UTF_8));

        Set<PosixFilePermission> perms = Sets.newHashSet(OWNER_READ, OWNER_WRITE);
        if (randomBoolean()) perms.add(OWNER_EXECUTE);
        if (randomBoolean()) perms.add(GROUP_EXECUTE);
        if (randomBoolean()) perms.add(OTHERS_EXECUTE);

        Files.setPosixFilePermissions(path, perms);

        try (PrintWriter writer = new PrintWriter(openAtomicMoveWriter(path))) {
            writer.printf(Locale.ROOT, "This is a test");
        }

        Set<PosixFilePermission> permissionsAfterWrite = Files.getPosixFilePermissions(path);
        assertThat(permissionsAfterWrite, is(perms));
    }

}
