/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;

/**
 */
public interface Compressor {

    String type();

    void configure(Settings settings);

    boolean isCompressed(BytesReference bytes);

    boolean isCompressed(byte[] data, int offset, int length);

    boolean isCompressed(ChannelBuffer buffer);

    boolean isCompressed(IndexInput in) throws IOException;

    /**
     * Uncompress the provided data, data can be detected as compressed using {@link #isCompressed(byte[], int, int)}.
     */
    byte[] uncompress(byte[] data, int offset, int length) throws IOException;

    /**
     * Compresses the provided data, data can be detected as compressed using {@link #isCompressed(byte[], int, int)}.
     */
    byte[] compress(byte[] data, int offset, int length) throws IOException;

    CompressedStreamInput streamInput(StreamInput in) throws IOException;

    CompressedStreamOutput streamOutput(StreamOutput out) throws IOException;

    CompressedIndexInput indexInput(IndexInput in) throws IOException;

    CompressedIndexOutput indexOutput(IndexOutput out) throws IOException;
}
