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

package org.elasticsearch.client.indexlifecycle;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;

public class ExplainLifecycleRequestTests extends AbstractWireSerializingTestCase<ExplainLifecycleRequest> {

    @Override
    protected ExplainLifecycleRequest createTestInstance() {
        ExplainLifecycleRequest request = new ExplainLifecycleRequest();
        if (randomBoolean()) {
            request.indices(generateRandomStringArray(20, 20, false, true));
        }
        if (randomBoolean()) {
            IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(),
                    randomBoolean(), randomBoolean(), randomBoolean());
            request.indicesOptions(indicesOptions);
        }
        return request;
    }

    @Override
    protected ExplainLifecycleRequest mutateInstance(ExplainLifecycleRequest instance) throws IOException {
        String[] indices = instance.indices();
        IndicesOptions indicesOptions = instance.indicesOptions();
        switch (between(0, 1)) {
        case 0:
            indices = randomValueOtherThanMany(i -> Arrays.equals(i, instance.indices()),
                    () -> generateRandomStringArray(20, 10, false, true));
            break;
        case 1:
            indicesOptions = randomValueOtherThan(indicesOptions, () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(),
                    randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        ExplainLifecycleRequest newRequest = new ExplainLifecycleRequest();
        newRequest.indices(indices);
        newRequest.indicesOptions(indicesOptions);
        return newRequest;
    }

    @Override
    protected Reader<ExplainLifecycleRequest> instanceReader() {
        return ExplainLifecycleRequest::new;
    }

}
