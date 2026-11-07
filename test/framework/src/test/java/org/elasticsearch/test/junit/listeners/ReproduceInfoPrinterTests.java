/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.junit.listeners;

import org.elasticsearch.test.ESTestCase;

import java.util.Properties;

import static org.hamcrest.Matchers.equalTo;

/**
 * A bwc test that ran against pinned DRA snapshots has to reproduce against the same snapshots, not against a source
 * build of the branch tip, which is what a reproduce line without the pins would run.
 */
public class ReproduceInfoPrinterTests extends ESTestCase {

    public void testBwcSnapshotPinsAreAppendedSorted() {
        Properties properties = new Properties();
        properties.setProperty("tests.bwc.mode", "auto");
        properties.setProperty("tests.bwc.dra.hash.9.4", "bbbb2222");
        properties.setProperty("tests.bwc.dra.hash.8.19", "aaaa1111");
        properties.setProperty("tests.bwc.version", "9.4.10");

        StringBuilder line = new StringBuilder("REPRODUCE WITH: ./gradlew \":x:v9.4.10#bwcTest\"");
        new ReproduceInfoPrinter.GradleMessageBuilder(line).appendBwcSnapshotPins(properties);

        assertThat(
            line.toString(),
            equalTo(
                "REPRODUCE WITH: ./gradlew \":x:v9.4.10#bwcTest\""
                    + " -Dtests.bwc.mode=auto -Dtests.bwc.dra.hash.8.19=aaaa1111 -Dtests.bwc.dra.hash.9.4=bbbb2222"
            )
        );
    }

    public void testNothingIsAppendedWithoutPins() {
        Properties properties = new Properties();
        properties.setProperty("tests.bwc.version", "9.4.10");

        StringBuilder line = new StringBuilder("REPRODUCE WITH: ./gradlew \":x:v9.4.10#bwcTest\"");
        new ReproduceInfoPrinter.GradleMessageBuilder(line).appendBwcSnapshotPins(properties);

        assertThat(line.toString(), equalTo("REPRODUCE WITH: ./gradlew \":x:v9.4.10#bwcTest\""));
    }
}
