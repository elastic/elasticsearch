/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * This exception is thrown if the File system is reported unhealthy by @{@link org.elasticsearch.monitor.fs.FsHealthService}
 * and this nodes needs to be removed from the cluster
 */

public class NodeHealthCheckFailureException extends ElasticsearchException {

    public NodeHealthCheckFailureException(String msg, Object... args) {
        super(msg, args);
    }

    public NodeHealthCheckFailureException(StreamInput in) throws IOException {
        super(in);
    }
}
