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

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class ExplainLifecycleRequestTests extends ESTestCase {

    public void testEqualsAndHashcode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), this::copy, this::mutateInstance);
    }

    public void testEmptyIndices() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, ExplainLifecycleRequest::new);
        assertThat(exception.getMessage(), equalTo("Must at least specify one index to explain"));
    }

    private ExplainLifecycleRequest createTestInstance() {
        ExplainLifecycleRequest request = new ExplainLifecycleRequest(generateRandomStringArray(20, 20, false, false));
        if (randomBoolean()) {
            IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(),
                    randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
            request.indicesOptions(indicesOptions);
        }
        return request;
    }

    private ExplainLifecycleRequest mutateInstance(ExplainLifecycleRequest instance) {
        String[] indices = instance.getIndices();
        IndicesOptions indicesOptions = instance.indicesOptions();
        switch (between(0, 1)) {
        case 0:
            indices = randomValueOtherThanMany(i -> Arrays.equals(i, instance.getIndices()),
                    () -> generateRandomStringArray(20, 10, false, false));
            break;
        case 1:
            indicesOptions = randomValueOtherThan(indicesOptions, () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(),
                    randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        ExplainLifecycleRequest newRequest = new ExplainLifecycleRequest(indices);
        newRequest.indicesOptions(indicesOptions);
        return newRequest;
    }

    private ExplainLifecycleRequest copy(ExplainLifecycleRequest original) {
        ExplainLifecycleRequest copy = new ExplainLifecycleRequest(original.getIndices());
        copy.indicesOptions(original.indicesOptions());
        return copy;
    }

}
