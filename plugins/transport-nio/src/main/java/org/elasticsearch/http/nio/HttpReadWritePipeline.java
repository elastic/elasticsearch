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

package org.elasticsearch.http.nio;

import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.timeout.ReadTimeoutHandler;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.FlushProducer;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.nio.WriteOperation;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;

public class HttpReadWritePipeline implements SocketChannelContext.ReadConsumer, FlushProducer {

    private final NettyAdaptor adaptor;

    public HttpReadWritePipeline() {
        // TODO: These use all the default settings level. The settings still need to be implemented.
        HttpRequestDecoder decoder = new HttpRequestDecoder(
            Math.toIntExact(new ByteSizeValue(4, ByteSizeUnit.KB).getBytes()),
            Math.toIntExact(new ByteSizeValue(8, ByteSizeUnit.KB).getBytes()),
            Math.toIntExact(new ByteSizeValue(8, ByteSizeUnit.KB).getBytes()));
        decoder.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
        HttpContentDecompressor decompressor = new HttpContentDecompressor();
        HttpResponseEncoder encoder = new HttpResponseEncoder();
        HttpObjectAggregator aggregator = new HttpObjectAggregator(Math.toIntExact(new ByteSizeValue(100, ByteSizeUnit.MB).getBytes()));
        HttpContentCompressor compressor = new HttpContentCompressor(3);

        adaptor = new NettyAdaptor(decoder, decompressor, encoder, aggregator, compressor);
    }

    @Override
    public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
        return adaptor.read(channelBuffer.sliceBuffersTo(channelBuffer.getIndex()));
    }

    @Override
    public void produceWrites(WriteOperation writeOperation) {
        adaptor.write(writeOperation);
    }

    @Override
    public FlushOperation pollFlushOperation() {
        return adaptor.pollFlushOperations();
    }

    @Override
    public void close() throws IOException {
        try {
            adaptor.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
