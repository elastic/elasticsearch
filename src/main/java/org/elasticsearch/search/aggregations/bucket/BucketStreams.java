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

package org.elasticsearch.search.aggregations.bucket;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class BucketStreams {

    private static ImmutableMap<BytesReference, Stream> STREAMS = ImmutableMap.of();

    /**
     * A stream that knows how to read a bucket from the input.
     */
    public static interface Stream<B extends MultiBucketsAggregation.Bucket> {
        B readResult(StreamInput in, BucketStreamContext context) throws IOException;
        BucketStreamContext getBucketStreamContext(B bucket);
    }

    /**
     * Registers the given stream and associate it with the given types.
     *
     * @param stream    The streams to register
     * @param types     The types associated with the streams
     */
    public static synchronized void registerStream(Stream stream, BytesReference... types) {
        MapBuilder<BytesReference, Stream> uStreams = MapBuilder.newMapBuilder(STREAMS);
        for (BytesReference type : types) {
            uStreams.put(type, stream);
        }
        STREAMS = uStreams.immutableMap();
    }

    /**
     * Returns the stream that is registered for the given type
     *
     * @param   type The given type
     * @return  The associated stream
     */
    public static Stream stream(BytesReference type) {
        return STREAMS.get(type);
    }

}
