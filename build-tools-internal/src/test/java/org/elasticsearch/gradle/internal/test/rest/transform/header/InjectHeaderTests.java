/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest.transform.header;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.gradle.internal.test.rest.transform.RestTestTransform;
import org.elasticsearch.gradle.internal.test.rest.transform.feature.InjectFeatureTests;
import org.elasticsearch.gradle.internal.test.rest.transform.headers.InjectHeaders;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InjectHeaderTests extends InjectFeatureTests {

    private static final Map<String, String> headers = Map.of(
        "Content-Type",
        "application/vnd.elasticsearch+json;compatible-with=8",
        "Accept",
        "application/vnd.elasticsearch+json;compatible-with=8"
    );

    /**
     * test file does not any headers defined
     */
    @Test
    public void testInjectHeadersNoPreExisting() throws Exception {
        String testName = "/rest/transform/header/without_existing_headers.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupDoesNotExist(tests);
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
        validateBodyHasHeaders(transformedTests, headers);
    }

    /**
     * test file has preexisting headers
     */
    @Test
    public void testInjectHeadersWithPreExisting() throws Exception {
        String testName = "/rest/transform/header/with_existing_headers.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupExist(tests);
        validateBodyHasHeaders(tests, Map.of("foo", "bar"));
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
        validateBodyHasHeaders(tests, Map.of("foo", "bar"));
        validateBodyHasHeaders(transformedTests, headers);
    }

    @Test
    public void testNotInjectingHeaders() throws Exception {
        String testName = "/rest/transform/header/with_operation_to_skip_adding_headers.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupExist(tests);
        validateBodyHasHeaders(tests, Map.of("foo", "bar"));

        List<RestTestTransform<?>> transformations = Collections.singletonList(
            new InjectHeaders(headers, Set.of(InjectHeaderTests::applyCondition))
        );
        List<ObjectNode> transformedTests = transformTests(tests, transformations);
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
        validateBodyHasHeaders(tests, Map.of("foo", "bar"));
        validateBodyHasHeaders(transformedTests, Map.of("foo", "bar"));
    }

    private static boolean applyCondition(ObjectNode doNodeValue) {
        final Iterator<String> fieldNamesIterator = doNodeValue.fieldNames();
        while (fieldNamesIterator.hasNext()) {
            final String fieldName = fieldNamesIterator.next();
            if (fieldName.startsWith("something_to_skip")) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected List<String> getKnownFeatures() {
        return Collections.singletonList("headers");
    }

    @Override
    protected List<RestTestTransform<?>> getTransformations() {
        return Collections.singletonList(new InjectHeaders(headers, Collections.emptySet()));
    }

    @Override
    protected boolean getHumanDebug() {
        return false;
    }
}
