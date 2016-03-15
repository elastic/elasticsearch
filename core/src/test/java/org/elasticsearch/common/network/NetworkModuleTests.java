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

package org.elasticsearch.common.network;

import org.elasticsearch.action.support.replication.ReplicationTask;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.Task.Status;

import java.io.IOException;

import static org.hamcrest.Matchers.sameInstance;

public class NetworkModuleTests extends ModuleTestCase {
    public void testRegisterTaskStatus() {
        NamedWriteableRegistry registry = new NamedWriteableRegistry();
        NetworkModule module = new NetworkModule(registry);

        // Builtin prototype comes back
        assertNotNull(registry.getPrototype(Task.Status.class, ReplicationTask.Status.PROTOTYPE.getWriteableName()));

        Task.Status dummy = new DummyTaskStatus();
        module.registerTaskStatus(dummy);
        assertThat((Task.Status) registry.getPrototype(Task.Status.class, "dummy"), sameInstance(dummy));
    }

    private class DummyTaskStatus implements Task.Status {
        @Override
        public String getWriteableName() {
            return "dummy";
        }

        @Override
        public Status readFrom(StreamInput in) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            throw new UnsupportedOperationException();
        }
    }
}
