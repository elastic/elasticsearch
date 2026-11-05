/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.local.AbstractLocalClusterFactory.Node;
import org.elasticsearch.test.cluster.util.Version;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Covers how test clusters pass keystore string settings to {@code elasticsearch-keystore add}.
 */
public class KeystoreAddTests {

    @Test
    public void testInputWithoutPasswordIsOneValuePerLine() {
        assertThat(Node.keystoreAddInput(null, List.of("a", "b")), is("a\nb"));
        assertThat(Node.keystoreAddInput("", List.of("a")), is("a"));
    }

    @Test
    public void testInputWithPasswordStartsWithPassword() {
        assertThat(Node.keystoreAddInput("secret", List.of("a", "b")), is("secret\na\nb"));
    }

    @Test
    public void testOneCallFromVersionWithMultipleSettingsSupport() {
        assertThat(Node.canAddKeystoreSettingsInOneCall(Version.fromString("7.7.0"), List.of("a", "b")), is(true));
        assertThat(Node.canAddKeystoreSettingsInOneCall(Version.fromString("9.1.0"), List.of("a", "b")), is(true));
        assertThat(Node.canAddKeystoreSettingsInOneCall(Version.fromString("7.6.2"), List.of("a", "b")), is(false));
    }

    @Test
    public void testMultiLineValuesNeedOneCallPerSetting() {
        assertThat(Node.canAddKeystoreSettingsInOneCall(Version.fromString("9.1.0"), List.of("a", "line1\nline2")), is(false));
        assertThat(Node.canAddKeystoreSettingsInOneCall(Version.fromString("9.1.0"), List.of("a\r")), is(false));
    }
}
