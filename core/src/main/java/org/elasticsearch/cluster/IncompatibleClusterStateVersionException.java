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

package org.elasticsearch.cluster;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Thrown by {@link Diffable#readDiffAndApply(org.elasticsearch.common.io.stream.StreamInput)} method
 */
public class IncompatibleClusterStateVersionException extends ElasticsearchException {
    public IncompatibleClusterStateVersionException(String msg) {
        super(msg);
    }

    public IncompatibleClusterStateVersionException(long expectedVersion, String expectedUuid, long receivedVersion, String receivedUuid) {
        super("Expected diff for version " + expectedVersion + " with uuid " + expectedUuid + " got version " + receivedVersion + " and uuid " + receivedUuid);
    }

    public IncompatibleClusterStateVersionException(StreamInput in) throws IOException{
        super(in);
    }
}
