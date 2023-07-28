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
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class VersionSensitiveWarningsHandlerTests extends ESTestCase {

    public void testSameVersionCluster() throws IOException {
        WarningsHandler handler = expectVersionSpecificWarnings(Set.of(Version.CURRENT), v -> v.current("expectedCurrent1"));
        assertFalse(handler.warningsShouldFailRequest(List.of("expectedCurrent1")));
        assertTrue(handler.warningsShouldFailRequest(List.of("expectedCurrent1", "unexpected")));
        assertTrue(handler.warningsShouldFailRequest(List.of()));

    }

    public void testMixedVersionCluster() throws IOException {
        WarningsHandler handler = expectVersionSpecificWarnings(
            Set.of(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()),
            v -> {
                v.current("expectedCurrent1");
                v.compatible("Expected legacy warning");
            }
        );
        assertFalse(handler.warningsShouldFailRequest(List.of("expectedCurrent1")));
        assertFalse(handler.warningsShouldFailRequest(List.of("Expected legacy warning")));
        assertFalse(handler.warningsShouldFailRequest(List.of("expectedCurrent1", "Expected legacy warning")));
        assertTrue(handler.warningsShouldFailRequest(List.of("expectedCurrent1", "Unexpected legacy warning")));
        assertTrue(handler.warningsShouldFailRequest(List.of("Unexpected legacy warning")));
        assertFalse(handler.warningsShouldFailRequest(List.of()));
    }

    private static WarningsHandler expectVersionSpecificWarnings(
        Set<Version> nodeVersions,
        Consumer<VersionSensitiveWarningsHandler> expectationsSetter
    ) {
        // Based on EsRestTestCase.expectVersionSpecificWarnings helper method but without ESRestTestCase dependency
        VersionSensitiveWarningsHandler warningsHandler = new VersionSensitiveWarningsHandler(nodeVersions);
        expectationsSetter.accept(warningsHandler);
        return warningsHandler;
    }
}
