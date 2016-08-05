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
package org.elasticsearch.transport.netty3;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.logging.InternalLogger;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;

/**
 */
public class Netty3Utils {

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
        InternalLoggerFactory.setDefaultFactory(new InternalLoggerFactory() {
            @Override
            public InternalLogger newInstance(String name) {
                name = name.replace("org.jboss.netty.", "netty3.").replace("org.jboss.netty.", "netty3.");
                return new Netty3InternalESLogger(Loggers.getLogger(name));
            }
        });

        ThreadRenamingRunnable.setThreadNameDeterminer(ES_THREAD_NAME_DETERMINER);

        // Netty 3 SelectorUtil wants to set this; however, it does not execute the property write
        // in a privileged block so we just do what Netty wants to do here
        final String key = "sun.nio.ch.bugLevel";
        final String buglevel = System.getProperty(key);
        if (buglevel == null) {
            try {
                AccessController.doPrivileged(new PrivilegedAction<Void>() {
                    @Override
                    @SuppressForbidden(reason = "to use System#setProperty to set sun.nio.ch.bugLevel")
                    public Void run() {
                        System.setProperty(key, "");
                        return null;
                    }
                });
            } catch (final SecurityException e) {
                Loggers.getLogger(Netty3Utils.class).debug("Unable to get/set System Property: {}", e, key);
            }
        }
    }

    public static void setup() {
    }

    /**
     * Turns the given BytesReference into a ChannelBuffer. Note: the returned ChannelBuffer will reference the internal
     * pages of the BytesReference. Don't free the bytes of reference before the ChannelBuffer goes out of scope.
     */
    public static ChannelBuffer toChannelBuffer(BytesReference reference) {
        if (reference.length() == 0) {
            return ChannelBuffers.EMPTY_BUFFER;
        }
        if (reference instanceof ChannelBufferBytesReference) {
            return ((ChannelBufferBytesReference) reference).toChannelBuffer();
        } else {
            final BytesRefIterator iterator = reference.iterator();
            BytesRef slice;
            final ArrayList<ChannelBuffer> buffers = new ArrayList<>();
            try {
                while ((slice = iterator.next()) != null) {
                    buffers.add(ChannelBuffers.wrappedBuffer(slice.bytes, slice.offset, slice.length));
                }
                return ChannelBuffers.wrappedBuffer(DEFAULT_GATHERING, buffers.toArray(new ChannelBuffer[buffers.size()]));
            } catch (IOException ex) {
                throw new AssertionError("no IO happens here", ex);
            }
        }
    }

    /**
     * Wraps the given ChannelBuffer with a BytesReference
     */
    public static BytesReference toBytesReference(ChannelBuffer channelBuffer) {
        return toBytesReference(channelBuffer, channelBuffer.readableBytes());
    }

    /**
     * Wraps the given ChannelBuffer with a BytesReference of a given size
     */
    public static BytesReference toBytesReference(ChannelBuffer channelBuffer, int size) {
        return new ChannelBufferBytesReference(channelBuffer, size);
    }
}
