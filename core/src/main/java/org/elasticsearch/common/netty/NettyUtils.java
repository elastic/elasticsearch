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
package org.elasticsearch.common.netty;

import org.elasticsearch.transport.netty.NettyInternalESLoggerFactory;
import org.jboss.netty.logging.InternalLogger;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;

/**
 */
public class NettyUtils {

    /**
     * Here we go....
     * <p>
     * When using the socket or file channel API to write or read using heap ByteBuffer, the sun.nio
     * package will convert it to a direct buffer before doing the actual operation. The direct buffer is
     * cached on an array of buffers under the nio.ch.Util$BufferCache on a thread local.
     * <p>
     * In netty specifically, if we send a single ChannelBuffer that is bigger than
     * SocketSendBufferPool#DEFAULT_PREALLOCATION_SIZE (64kb), it will just convert the ChannelBuffer
     * to a ByteBuffer and send it. The problem is, that then same size DirectByteBuffer will be
     * allocated (or reused) and kept around on a thread local in the sun.nio BufferCache. If very
     * large buffer is sent, imagine a 10mb one, then a 10mb direct buffer will be allocated as an
     * entry within the thread local buffers.
     * <p>
     * In ES, we try and page the buffers allocated, all serialized data uses {@link org.elasticsearch.common.bytes.PagedBytesReference}
     * typically generated from {@link org.elasticsearch.common.io.stream.BytesStreamOutput}. When sending it over
     * to netty, it creates a {@link org.jboss.netty.buffer.CompositeChannelBuffer} that wraps the relevant pages.
     * <p>
     * The idea with the usage of composite channel buffer is that a single large buffer will not be sent over
     * to the sun.nio layer. But, this will only happen if the composite channel buffer is created with a gathering
     * flag set to true. In such a case, the GatheringSendBuffer is used in netty, resulting in calling the sun.nio
     * layer with a ByteBuffer array.
     * <p>
     * This, potentially would have been as disastrous if the sun.nio layer would have tried to still copy over
     * all of it to a direct buffer. But, the write(ByteBuffer[]) API (see sun.nio.ch.IOUtil), goes one buffer
     * at a time, and gets a temporary direct buffer from the BufferCache, up to a limit of IOUtil#IOV_MAX (which
     * is 1024 on most OSes). This means that there will be a max of 1024 direct buffer per thread.
     * <p>
     * This is still less than optimal to be honest, since it means that if not all data was written successfully
     * (1024 paged buffers), then the rest of the data will need to be copied over again to the direct buffer
     * and re-transmitted, but its much better than trying to send the full large buffer over and over again.
     * <p>
     * In ES, we use by default, in our paged data structures, a page of 16kb, so this is not so terrible.
     * <p>
     * Note, on the read size of netty, it uses a single direct buffer that is defined in both the transport
     * and http configuration (based on the direct memory available), and the upstream handlers (SizeHeaderFrameDecoder,
     * or more specifically the FrameDecoder base class) makes sure to use a cumulation buffer and not copy it
     * over all the time.
     * <p>
     * TODO: potentially, a more complete solution would be to write a netty channel handler that is the last
     * in the pipeline, and if the buffer is composite, verifies that its a gathering one with reasonable
     * sized pages, and if its a single one, makes sure that it gets sliced and wrapped in a composite
     * buffer.
     */
    public static final boolean DEFAULT_GATHERING = true;

    private static EsThreadNameDeterminer ES_THREAD_NAME_DETERMINER = new EsThreadNameDeterminer();

    public static class EsThreadNameDeterminer implements ThreadNameDeterminer {
        @Override
        public String determineThreadName(String currentThreadName, String proposedThreadName) throws Exception {
            // we control the thread name with a context, so use both
            return currentThreadName + "{" + proposedThreadName + "}";
        }
    }

    static {
        InternalLoggerFactory.setDefaultFactory(new NettyInternalESLoggerFactory() {
            @Override
            public InternalLogger newInstance(String name) {
                return super.newInstance(name.replace("org.jboss.netty.", "netty.").replace("org.jboss.netty.", "netty."));
            }
        });

        ThreadRenamingRunnable.setThreadNameDeterminer(ES_THREAD_NAME_DETERMINER);
    }

    public static void setup() {

    }
}
