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
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;

public class TransportTestsScannerTests {
    @Test
    public void testFindOneFile() throws IOException {
        String writeable = "org.elasticsearch.gradle.internal.precommit.transport.test_classes.Writeable";
        Set<String> transportTestClassesRoots = Set.of(
            "org.elasticsearch.gradle.internal.precommit.transport.test_classes.test_roots.TransportTestBaseClass"
        );

        TransportTestsScanner scanner = new TransportTestsScanner(Set.of(), writeable, transportTestClassesRoots);
        Set<File> all = Arrays.stream(System.getProperty("java.class.path").split(System.getProperty("path.separator")))
            .filter(s -> s.contains("build-tools-internal"))
            .filter(s -> s.contains("test" + File.separator + "classes"))
            .map(s -> new File(s))
            .collect(Collectors.toSet());

        Set<String> transportClassesMissingTests = scanner.findTransportClassesMissingTests(all, all, Set.of(), Set.of());

        assertThat(
            transportClassesMissingTests,
            Matchers.not(Matchers.hasItem("org.elasticsearch.gradle.internal.precommit.transport.test_classes.prod.ExampleImpl"))
        );
    }

    @Test
    public void testFindTransportClassWithNonDirectImplements() throws IOException {
        String writeable = "org.elasticsearch.gradle.internal.precommit.transport.test_classes.Writeable";
        Set<String> transportTestClassesRoots = Set.of(
            "org.elasticsearch.gradle.internal.precommit.transport.test_classes.test_roots.TransportTestBaseClass"
        );

        TransportTestsScanner scanner = new TransportTestsScanner(Set.of(), writeable, transportTestClassesRoots);
        Set<File> all = Arrays.stream(System.getProperty("java.class.path").split(System.getProperty("path.separator")))
            .filter(s -> s.contains("build-tools-internal"))
            .filter(s -> s.contains("test" + File.separator + "classes"))
            .map(s -> new File(s))
            .collect(Collectors.toSet());

        Set<String> transportClassesMissingTests = scanner.findTransportClassesMissingTests(all, all, Set.of(), Set.of());

        assertThat(
            transportClassesMissingTests,
            Matchers.not(Matchers.hasItem("org.elasticsearch.gradle.internal.precommit.transport.test_classes.prod.ExampleSubclass"))
        );
    }

    @Test
    public void testFindNestedTransportClass() throws IOException {
        String writeable = "org.elasticsearch.gradle.internal.precommit.transport.test_classes.Writeable";
        Set<String> transportTestClassesRoots = Set.of(
            "org.elasticsearch.gradle.internal.precommit.transport.test_classes.test_roots.TransportTestBaseClass"
        );

        TransportTestsScanner scanner = new TransportTestsScanner(Set.of(), writeable, transportTestClassesRoots);
        Set<File> all = Arrays.stream(System.getProperty("java.class.path").split(System.getProperty("path.separator")))
            .filter(s -> s.contains("build-tools-internal"))
            .filter(s -> s.contains("test" + File.separator + "classes"))
            .map(s -> new File(s))
            .collect(Collectors.toSet());

        Set<String> transportClassesMissingTests = scanner.findTransportClassesMissingTests(all, all, Set.of(), Set.of());

        assertThat(
            transportClassesMissingTests,
            Matchers.not(
                Matchers.hasItem(
                    "org.elasticsearch.gradle.internal.precommit.transport.test_classes.prod.EnclosingInnerTransport$Transport"
                )
            )
        );
    }

    @Test
    public void testAbstractTransportClass() throws IOException {
        String writeable = "org.elasticsearch.gradle.internal.precommit.transport.test_classes.Writeable";
        Set<String> transportTestClassesRoots = Set.of(
            "org.elasticsearch.gradle.internal.precommit.transport.test_classes.test_roots.TransportTestBaseClass"
        );

        TransportTestsScanner scanner = new TransportTestsScanner(Set.of(), writeable, transportTestClassesRoots);
        Set<File> all = Arrays.stream(System.getProperty("java.class.path").split(System.getProperty("path.separator")))
            .filter(s -> s.contains("build-tools-internal"))
            .filter(s -> s.contains("test" + File.separator + "classes"))
            .map(s -> new File(s))
            .collect(Collectors.toSet());

        Set<String> transportClassesMissingTests = scanner.findTransportClassesMissingTests(all, all, Set.of(), Set.of());

        assertThat(
            transportClassesMissingTests,
            Matchers.not(Matchers.hasItem("org.elasticsearch.gradle.internal.precommit.transport.test_classes.prod.AbstractTransportClass"))
        );
    }

    @Test
    public void testSkipMisssingClasses() throws IOException {
        String writeable = "org.elasticsearch.gradle.internal.precommit.transport.test_classes.Writeable";
        Set<String> transportTestClassesRoots = Set.of(
            "org.elasticsearch.gradle.internal.precommit.transport.test_classes.test_roots.TransportTestBaseClass"
        );

        Set<File> all = Arrays.stream(System.getProperty("java.class.path").split(System.getProperty("path.separator")))
            .filter(s -> s.contains("build-tools-internal"))
            .filter(s -> s.contains("test" + File.separator + "classes"))
            .map(s -> new File(s))
            .collect(Collectors.toSet());

        TransportTestsScanner scanner = new TransportTestsScanner(Set.of(), writeable, transportTestClassesRoots);
        Set<String> transportClassesMissingTests = scanner.findTransportClassesMissingTests(all, all, Set.of(), Set.of());
        assertThat(
            transportClassesMissingTests,
            Matchers.hasItem("org.elasticsearch.gradle.internal.precommit.transport.test_classes.prod.TransportWithoutTest")
        );

        //the class without a test should be skipped
        Set<String> missingClassesToSkip =
            Set.of("org.elasticsearch.gradle.internal.precommit.transport.test_classes.prod.TransportWithoutTest");
        scanner = new TransportTestsScanner(missingClassesToSkip, writeable, transportTestClassesRoots);
        transportClassesMissingTests = scanner.findTransportClassesMissingTests(all, all, Set.of(), Set.of());
        assertThat(
            transportClassesMissingTests,
            Matchers.not(Matchers.hasItem("org.elasticsearch.gradle.internal.precommit.transport.test_classes.prod.TransportWithoutTest"))
        );

    }
}
