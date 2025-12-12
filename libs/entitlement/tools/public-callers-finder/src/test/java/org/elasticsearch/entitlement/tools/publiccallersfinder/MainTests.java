/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools.publiccallersfinder;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.hasItem;

public class MainTests extends ESTestCase {

    public void testTransitiveFindsDeepCallChain() throws Exception {
        URI uri = getClass().getResource("shutdown-halt-input.tsv").toURI();
        String filePath = PathUtils.get(uri).toAbsolutePath().toString();
        String output = runMain(filePath, "--transitive");
        assertThat(findClassesWithAccess(output), hasItem("java/lang/System"));
    }

    private Set<String> findClassesWithAccess(String output) {
        return output.lines()
            .filter(line -> line.isEmpty() == false)
            .map(line -> line.split("\t"))
            .map(parts -> parts[3])
            .collect(Collectors.toSet());
    }

    @SuppressForbidden(reason = "Main prints to System.out, so we must capture it")
    private String runMain(String... args) throws Exception {
        PrintStream original = System.out;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(captured, false, StandardCharsets.UTF_8));
            Main.main(args);
            return captured.toString(StandardCharsets.UTF_8);
        } finally {
            System.setOut(original);
        }
    }
}
