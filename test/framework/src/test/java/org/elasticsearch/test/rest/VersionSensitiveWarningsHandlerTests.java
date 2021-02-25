/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.Version;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ESRestTestCase.VersionSensitiveWarningsHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

public class VersionSensitiveWarningsHandlerTests extends ESTestCase {

    public void testSameVersionCluster() throws IOException {
        Set<Version> nodeVersions= new HashSet<>();
        nodeVersions.add(Version.CURRENT);
        WarningsHandler handler = expectVersionSpecificWarnings(nodeVersions, (v)->{
            v.current("expectedCurrent1");
        });
        assertFalse(handler.warningsShouldFailRequest(Arrays.asList("expectedCurrent1")));
        assertTrue(handler.warningsShouldFailRequest(Arrays.asList("expectedCurrent1", "unexpected")));
        assertTrue(handler.warningsShouldFailRequest(Collections.emptyList()));

    }
    public void testMixedVersionCluster() throws IOException {
        Set<Version> nodeVersions= new HashSet<>();
        nodeVersions.add(Version.CURRENT);
        nodeVersions.add(Version.CURRENT.minimumIndexCompatibilityVersion());
        WarningsHandler handler = expectVersionSpecificWarnings(nodeVersions, (v)->{
            v.current("expectedCurrent1");
            v.compatible("Expected legacy warning");
        });
        assertFalse(handler.warningsShouldFailRequest(Arrays.asList("expectedCurrent1")));
        assertFalse(handler.warningsShouldFailRequest(Arrays.asList("Expected legacy warning")));
        assertFalse(handler.warningsShouldFailRequest(Arrays.asList("expectedCurrent1", "Expected legacy warning")));
        assertTrue(handler.warningsShouldFailRequest(Arrays.asList("expectedCurrent1", "Unexpected legacy warning")));
        assertTrue(handler.warningsShouldFailRequest(Arrays.asList("Unexpected legacy warning")));
        assertFalse(handler.warningsShouldFailRequest(Collections.emptyList()));
    }

    private static WarningsHandler expectVersionSpecificWarnings(Set<Version> nodeVersions,
            Consumer<VersionSensitiveWarningsHandler> expectationsSetter) {
        //Based on EsRestTestCase.expectVersionSpecificWarnings helper method but without ESRestTestCase dependency
        VersionSensitiveWarningsHandler warningsHandler = new VersionSensitiveWarningsHandler(nodeVersions);
        expectationsSetter.accept(warningsHandler);
        return warningsHandler;
    }
}
