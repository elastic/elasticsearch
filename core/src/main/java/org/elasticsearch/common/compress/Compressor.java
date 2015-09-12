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

package org.elasticsearch.common.compress;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;

/**
 */
public interface Compressor {

    boolean isCompressed(BytesReference bytes);

    boolean isCompressed(ChannelBuffer buffer);

    StreamInput streamInput(StreamInput in) throws IOException;

    StreamOutput streamOutput(StreamOutput out) throws IOException;

    /**
     * @deprecated Used for backward comp. since we now use Lucene compressed codec.
     */
    @Deprecated
    boolean isCompressed(IndexInput in) throws IOException;

    /**
     * @deprecated Used for backward comp. since we now use Lucene compressed codec.
     */
    @Deprecated
    CompressedIndexInput indexInput(IndexInput in) throws IOException;
}
