/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.match;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.gradle.internal.test.rest.transform.AssertObjectNodes;
import org.elasticsearch.gradle.internal.test.rest.transform.TransformTests;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class ReplaceKeyInMatchTests extends TransformTests {

    @Test
    public void testReplaceKeyInMatch() throws Exception {

        String test_original = "/rest/transform/match/key_replace/replace_key_in_match_original.yml";
        List<ObjectNode> tests = getTests(test_original);

        String test_transformed = "/rest/transform/match/key_replace/replace_key_in_match_transformed.yml";
        List<ObjectNode> expectedTransformation = getTests(test_transformed);

        List<ObjectNode> transformedTests = transformTests(
            tests,
            Collections.singletonList(new ReplaceKeyInMatch("match.key.to_replace", "match.key.replaced", null))
        );

        AssertObjectNodes.areEqual(transformedTests, expectedTransformation);
    }
}
