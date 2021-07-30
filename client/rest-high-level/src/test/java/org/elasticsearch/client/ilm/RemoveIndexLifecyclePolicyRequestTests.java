/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ilm;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class RemoveIndexLifecyclePolicyRequestTests extends ESTestCase {

    public void testNullIndices() {
        expectThrows(NullPointerException.class, () -> new RemoveIndexLifecyclePolicyRequest(null));
    }

    public void testNullIndicesOptions() {
        expectThrows(NullPointerException.class, () -> new RemoveIndexLifecyclePolicyRequest(Collections.emptyList(), null));
    }

    public void testValidate() {
        RemoveIndexLifecyclePolicyRequest request = new RemoveIndexLifecyclePolicyRequest(Collections.emptyList());
        assertFalse(request.validate().isPresent());
    }

    protected RemoveIndexLifecyclePolicyRequest createInstance() {
        if (randomBoolean()) {
            return new RemoveIndexLifecyclePolicyRequest(Arrays.asList(generateRandomStringArray(20, 20, false)),
                    IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(),
                    randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        } else {
            return new RemoveIndexLifecyclePolicyRequest(Arrays.asList(generateRandomStringArray(20, 20, false)));
        }
    }

    private RemoveIndexLifecyclePolicyRequest copyInstance(RemoveIndexLifecyclePolicyRequest req) {
        return new RemoveIndexLifecyclePolicyRequest(new ArrayList<>(req.indices()), IndicesOptions.fromOptions(
                req.indicesOptions().ignoreUnavailable(), req.indicesOptions().allowNoIndices(),
                req.indicesOptions().expandWildcardsOpen(), req.indicesOptions().expandWildcardsClosed(),
                req.indicesOptions().allowAliasesToMultipleIndices(), req.indicesOptions().forbidClosedIndices(),
                req.indicesOptions().ignoreAliases(), req.indicesOptions().ignoreThrottled()));
    }

    private RemoveIndexLifecyclePolicyRequest mutateInstance(RemoveIndexLifecyclePolicyRequest req) {
        if (randomBoolean()) {
            return new RemoveIndexLifecyclePolicyRequest(req.indices(),
                    randomValueOtherThan(req.indicesOptions(), () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(),
                    randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())));
        } else {
            return new RemoveIndexLifecyclePolicyRequest(
                    randomValueOtherThan(req.indices(), () -> Arrays.asList(generateRandomStringArray(20, 20, false))),
                    req.indicesOptions());
        }
    }

    public void testEqualsAndHashCode() {
        for (int count = 0; count < 100; ++count) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(createInstance(), this::copyInstance, this::mutateInstance);
        }
    }
}
