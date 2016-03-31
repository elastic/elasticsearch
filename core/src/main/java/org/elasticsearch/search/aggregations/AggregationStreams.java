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
package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Once was a registry similar to NamedWriteableRegistry. In process of being migrated to NamedWriteableRegistry.
 */
public class AggregationStreams {
    /**
     * A stream that knows how to read an aggregation from the input.
     */
    public static interface Stream extends Writeable.Reader<InternalAggregation> {
        InternalAggregation readResult(StreamInput in) throws IOException;

        // Shim so we can cut aggregations over more carefully
        @Override
        default InternalAggregation read(StreamInput in) throws IOException {
            return readResult(in);
        }
    }
}
