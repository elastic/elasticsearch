/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
