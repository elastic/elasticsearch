/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit.transport;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;

public class TransportTestsScannerClasses {
    String writeable = "org.elasticsearch.gradle.internal.precommit.transport.test_classes.Writeable";

    @Test
    public void testFindOneFile() {
        TransportClassesScanner scanner = new TransportClassesScanner(Set.of(), writeable);
        Set<File> all = Arrays.stream(System.getProperty("java.class.path").split(System.getProperty("path.separator")))
            .filter(s -> s.contains("build-tools-internal"))
            .filter(s -> s.contains("test" + File.separator + "classes"))
            .map(s -> new File(s))
            .collect(Collectors.toSet());

        Set<String> transportClasses = scanner.findTransportClasses(all, Set.of());

        assertThat(
            transportClasses,
            Matchers.containsInAnyOrder(
                "org.elasticsearch.gradle.internal.precommit.transport.test_classes.prod.ExampleImpl",
                "org.elasticsearch.gradle.internal.precommit.transport.test_classes.prod.TransportWithoutTest",
                "org.elasticsearch.gradle.internal.precommit.transport.test_classes.prod.ExampleSubclass",
                "org.elasticsearch.gradle.internal.precommit.transport.test_classes.prod.EnclosingInnerTransport$Transport"
            )
        );
    }

    @Test
    public void testSkipMisssingClasses() {
        Set<File> all = Arrays.stream(System.getProperty("java.class.path").split(System.getProperty("path.separator")))
            .filter(s -> s.contains("build-tools-internal"))
            .filter(s -> s.contains("test" + File.separator + "classes"))
            .map(s -> new File(s))
            .collect(Collectors.toSet());

        TransportClassesScanner scanner = new TransportClassesScanner(Set.of(), writeable);
        Set<String> transportClasses = scanner.findTransportClasses(all, Set.of());
        assertThat(
            transportClasses,
            Matchers.hasItem("org.elasticsearch.gradle.internal.precommit.transport.test_classes.prod.TransportWithoutTest")
        );

        // the class without a test should be skipped
        Set<String> missingClassesToSkip = Set.of(
            "org.elasticsearch.gradle.internal.precommit.transport.test_classes.prod.TransportWithoutTest"
        );
        scanner = new TransportClassesScanner(missingClassesToSkip, writeable);
        transportClasses = scanner.findTransportClasses(all, Set.of());
        assertThat(
            transportClasses,
            Matchers.not(Matchers.hasItem("org.elasticsearch.gradle.internal.precommit.transport.test_classes.prod.TransportWithoutTest"))
        );
    }
}
