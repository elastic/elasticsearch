/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.junit.listener;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import org.junit.runner.Description;
import org.junit.runner.notification.Failure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

@TestMethodProviders({ JUnit3MethodProvider.class })
public class ReproduceInfoPrinterTests extends RandomizedTest {

    public void testTestNameHasNoIterationSeed() {
        String message = messageFor("testFoo {seed=[A1:B2]}");

        assertThat(message, containsString("--tests \"org.example.SomeTests.testFoo\""));
        assertThat(message, not(containsString("{seed=")));
        assertThat(message, containsString(" -Dtests.seed=A1:B2 "));
    }

    public void testParametersOfTheTestAreKept() {
        String message = messageFor("testFoo {p=a seed=[A1:B2]}");

        assertThat(message, containsString("--tests \"org.example.SomeTests.testFoo {p=a}\""));
        assertThat(message, containsString(" -Dtests.seed=A1:B2 "));
    }

    public void testTestsThatAreNotRepeatedKeepTheirName() {
        String message = messageFor("testFoo {p=a}");

        assertThat(message, containsString("--tests \"org.example.SomeTests.testFoo {p=a}\""));
        assertThat(message, containsString("-Dtests.seed="));
    }

    private static String messageFor(String testName) {
        Description description = Description.createTestDescription("org.example.SomeTests", testName);
        return ReproduceInfoPrinter.reproduceMessage(new Failure(description, new AssertionError("boom")));
    }
}
