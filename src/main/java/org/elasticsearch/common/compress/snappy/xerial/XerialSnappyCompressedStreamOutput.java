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

package org.elasticsearch.common.compress.snappy.xerial;

import org.elasticsearch.common.compress.snappy.SnappyCompressedStreamOutput;
import org.elasticsearch.common.compress.snappy.SnappyCompressorContext;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 */
public class XerialSnappyCompressedStreamOutput extends SnappyCompressedStreamOutput {

    public XerialSnappyCompressedStreamOutput(StreamOutput out, SnappyCompressorContext context) throws IOException {
        super(out, context);
    }

    @Override
    protected void compress(byte[] data, int offset, int len, StreamOutput out) throws IOException {
        int compressedLength = Snappy.rawCompress(data, offset, len, compressedBuffer, 0);
        // use uncompressed input if less than 12.5% compression
        if (compressedLength >= (len - (len / 8))) {
            out.writeBoolean(false);
            out.writeVInt(len);
            out.writeBytes(data, offset, len);
        } else {
            out.writeBoolean(true);
            out.writeVInt(compressedLength);
            out.writeBytes(compressedBuffer, 0, compressedLength);
        }
    }
}
