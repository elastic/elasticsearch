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

package org.elasticsearch.protocol.xpack.indexlifecycle;

import org.elasticsearch.test.AbstractStreamableTestCase;

import java.io.IOException;

public class PutOperationModeRequestTests extends AbstractStreamableTestCase<PutOperationModeRequest> {

    @Override
    protected PutOperationModeRequest createBlankInstance() {
        return new PutOperationModeRequest();
    }

    @Override
    protected PutOperationModeRequest createTestInstance() {
        return new PutOperationModeRequest(randomFrom(OperationMode.STOPPING, OperationMode.RUNNING));
    }

    @Override
    protected PutOperationModeRequest mutateInstance(PutOperationModeRequest instance) throws IOException {
        return new PutOperationModeRequest(
                randomValueOtherThan(instance.getMode(), () -> randomFrom(OperationMode.STOPPING, OperationMode.RUNNING)));
    }

    public void testNullMode() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new PutOperationModeRequest(null));
        assertEquals("mode cannot be null", exception.getMessage());
    }

    public void testStoppedMode() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> new PutOperationModeRequest(OperationMode.STOPPED));
        assertEquals("cannot directly stop index-lifecycle", exception.getMessage());
    }

    public void testValidate() {
        PutOperationModeRequest request = createTestInstance();
        assertNull(request.validate());
    }

}
