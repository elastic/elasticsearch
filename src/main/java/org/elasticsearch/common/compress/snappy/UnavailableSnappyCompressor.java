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

package org.elasticsearch.common.compress.snappy;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.compress.CompressedIndexInput;
import org.elasticsearch.common.compress.CompressedIndexOutput;
import org.elasticsearch.common.compress.CompressedStreamInput;
import org.elasticsearch.common.compress.CompressedStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 */
public class UnavailableSnappyCompressor extends SnappyCompressor {

    @Override
    public String type() {
        return "snappy";
    }

    @Override
    protected int maxCompressedLength(int length) {
        return length;
    }

    @Override
    public byte[] uncompress(byte[] data, int offset, int length) throws IOException {
        throw new ElasticSearchIllegalStateException("snappy unavailable");
    }

    @Override
    public byte[] compress(byte[] data, int offset, int length) throws IOException {
        throw new ElasticSearchIllegalStateException("snappy unavailable");
    }

    @Override
    public CompressedStreamInput streamInput(StreamInput in) throws IOException {
        throw new ElasticSearchIllegalStateException("snappy unavailable");
    }

    @Override
    public CompressedStreamOutput streamOutput(StreamOutput out) throws IOException {
        throw new ElasticSearchIllegalStateException("snappy unavailable");
    }

    @Override
    public CompressedIndexInput indexInput(IndexInput in) throws IOException {
        throw new ElasticSearchIllegalStateException("snappy unavailable");
    }

    @Override
    public CompressedIndexOutput indexOutput(IndexOutput out) throws IOException {
        throw new ElasticSearchIllegalStateException("snappy unavailable");
    }
}
