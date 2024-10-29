/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.xcontent.XContentLocation;

import java.io.IOException;

public class IsAfterAssertionTests extends AbstractClientYamlTestFragmentParserTestCase {

    public void testParseIsAfterAssertionWithNonInstantValue() {
        XContentLocation xContentLocation = new XContentLocation(0, 0);
        IsAfterAssertion isAfterAssertion = new IsAfterAssertion(xContentLocation, "some_field", "non instant value");

        expectThrows(AssertionError.class, () -> isAfterAssertion.doAssert("2022-05-25T12:30:00.000Z", "non instant value"));
    }

    public void testIsAfter() {
        String field = "some_field";

        // actual value one year after value to test against
        String actualValue = "2022-05-25T12:30:00.000Z";
        String expectedValue = "2021-05-25T12:30:00.000Z";

        XContentLocation xContentLocation = new XContentLocation(0, 0);
        IsAfterAssertion isAfterAssertion = new IsAfterAssertion(xContentLocation, field, expectedValue);

        isAfterAssertion.doAssert(actualValue, expectedValue);
    }

    public void testIsNotAfter() {
        String field = "some_field";

        // actual value one year before value to test against
        String actualValue = "2020-05-25T12:30:00.000Z";
        String expectedValue = "2021-05-25T12:30:00.000Z";

        XContentLocation xContentLocation = new XContentLocation(0, 0);
        IsAfterAssertion isAfterAssertion = new IsAfterAssertion(xContentLocation, field, expectedValue);

        expectThrows(AssertionError.class, () -> isAfterAssertion.doAssert(actualValue, expectedValue));
    }

    public void testActualValueIsNull() {
        XContentLocation xContentLocation = new XContentLocation(0, 0);
        IsAfterAssertion isAfterAssertion = new IsAfterAssertion(xContentLocation, "field", "2021-05-25T12:30:00.000Z");

        expectThrows(AssertionError.class, () -> isAfterAssertion.doAssert(null, "2021-05-25T12:30:00.000Z"));
    }

    public void testExpectedValueIsNull() throws IOException {
        XContentLocation xContentLocation = new XContentLocation(0, 0);
        IsAfterAssertion isAfterAssertion = new IsAfterAssertion(xContentLocation, "field", "2021-05-25T12:30:00.000Z");

        expectThrows(AssertionError.class, () -> isAfterAssertion.doAssert("2021-05-25T12:30:00.000Z", null));
    }
}
