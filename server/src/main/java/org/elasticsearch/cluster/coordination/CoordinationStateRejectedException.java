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

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * This exception is thrown when rejecting state transitions on the {@link CoordinationState} object,
 * for example when receiving a publish request with the wrong term or version.
 * Occurrences of this exception don't always signal failures, but can often be just caused by the
 * asynchronous, distributed nature of the system. They will, for example, naturally happen during
 * leader election, if multiple nodes are trying to become leader at the same time.
 */
public class CoordinationStateRejectedException extends ElasticsearchException {
    public CoordinationStateRejectedException(String msg, Object... args) {
        super(msg, args);
    }

    public CoordinationStateRejectedException(StreamInput in) throws IOException {
        super(in);
    }
}
