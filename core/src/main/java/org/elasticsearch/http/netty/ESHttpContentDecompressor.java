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

package org.elasticsearch.http.netty;

import org.elasticsearch.transport.TransportException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.embedder.DecoderEmbedder;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;
import org.jboss.netty.handler.codec.http.HttpHeaders;

public class ESHttpContentDecompressor extends HttpContentDecompressor {
    private final boolean compression;

    public ESHttpContentDecompressor(boolean compression) {
        super();
        this.compression = compression;
    }

    @Override
    protected DecoderEmbedder<ChannelBuffer> newContentDecoder(String contentEncoding) throws Exception {
        if (compression) {
            // compression is enabled so handle the request according to the headers (compressed and uncompressed)
            return super.newContentDecoder(contentEncoding);
        } else {
            // if compression is disabled only allow "indentity" (uncompressed) requests
            if (HttpHeaders.Values.IDENTITY.equals(contentEncoding)) {
                // nothing to handle here
                return null;
            } else {
                throw new TransportException("Support for compressed content is disabled. You can enable it with http.compression=true");
            }
        }
    }
}