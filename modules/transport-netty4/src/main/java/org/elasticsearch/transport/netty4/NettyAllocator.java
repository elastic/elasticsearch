/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class NettyAllocator {

    private static final Logger logger = LogManager.getLogger(NettyAllocator.class);
    private static final AtomicBoolean descriptionLogged = new AtomicBoolean(false);

    private static final long SUGGESTED_MAX_ALLOCATION_SIZE;
    private static final ByteBufAllocator ALLOCATOR;
    private static final Recycler<BytesRef> RECYCLER;
    private static final String DESCRIPTION;

    private static final String USE_UNPOOLED = "es.use_unpooled_allocator";
    private static final String USE_NETTY_DEFAULT = "es.unsafe.use_netty_default_allocator";
    private static final String USE_NETTY_DEFAULT_CHUNK = "es.unsafe.use_netty_default_chunk_and_page_size";

    static {
        ByteBufAllocator allocator;
        if (Booleans.parseBoolean(System.getProperty(USE_NETTY_DEFAULT), false)) {
            allocator = ByteBufAllocator.DEFAULT;
            SUGGESTED_MAX_ALLOCATION_SIZE = 1024 * 1024;
            DESCRIPTION = "[name=netty_default, suggested_max_allocation_size="
                + ByteSizeValue.ofBytes(SUGGESTED_MAX_ALLOCATION_SIZE)
                + ", factors={es.unsafe.use_netty_default_allocator=true}]";
        } else {
            final long heapSizeInBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
            final boolean g1gcEnabled = Boolean.parseBoolean(JvmInfo.jvmInfo().useG1GC());
            final long g1gcRegionSizeInBytes = JvmInfo.jvmInfo().getG1RegionSize();
            final boolean g1gcRegionSizeIsKnown = g1gcRegionSizeInBytes != -1;
            ByteSizeValue heapSize = ByteSizeValue.ofBytes(heapSizeInBytes);
            ByteSizeValue g1gcRegionSize = ByteSizeValue.ofBytes(g1gcRegionSizeInBytes);

            ByteBufAllocator delegate;
            if (useUnpooled(heapSizeInBytes, g1gcEnabled, g1gcRegionSizeIsKnown, g1gcRegionSizeInBytes)) {
                delegate = UnpooledByteBufAllocator.DEFAULT;
                if (g1gcEnabled && g1gcRegionSizeIsKnown) {
                    // Suggested max allocation size 1/4 of region size. Guard against unknown edge cases
                    // where this value would be less than 256KB.
                    SUGGESTED_MAX_ALLOCATION_SIZE = Math.max(g1gcRegionSizeInBytes >> 2, 256 * 1024);
                } else {
                    SUGGESTED_MAX_ALLOCATION_SIZE = 1024 * 1024;
                }
                DESCRIPTION = "[name=unpooled, suggested_max_allocation_size="
                    + ByteSizeValue.ofBytes(SUGGESTED_MAX_ALLOCATION_SIZE)
                    + ", factors={es.unsafe.use_unpooled_allocator="
                    + System.getProperty(USE_UNPOOLED)
                    + ", g1gc_enabled="
                    + g1gcEnabled
                    + ", g1gc_region_size="
                    + g1gcRegionSize
                    + ", heap_size="
                    + heapSize
                    + "}]";
            } else {
                int nHeapArena = PooledByteBufAllocator.defaultNumHeapArena();
                int pageSize;
                int maxOrder;
                if (useDefaultChunkAndPageSize()) {
                    pageSize = PooledByteBufAllocator.defaultPageSize();
                    maxOrder = PooledByteBufAllocator.defaultMaxOrder();
                } else {
                    pageSize = 8192;
                    if (g1gcEnabled == false || g1gcRegionSizeIsKnown == false || g1gcRegionSizeInBytes >= (4 * 1024 * 1024)) {
                        // This combined with a 8192 page size = 1 MB chunk sizes
                        maxOrder = 7;
                    } else if (g1gcRegionSizeInBytes >= (2 * 1024 * 1024)) {
                        // This combined with a 8192 page size = 512 KB chunk sizes
                        maxOrder = 6;
                    } else {
                        // This combined with a 8192 page size = 256 KB chunk sizes
                        maxOrder = 5;
                    }
                }
                int smallCacheSize = PooledByteBufAllocator.defaultSmallCacheSize();
                int normalCacheSize = PooledByteBufAllocator.defaultNormalCacheSize();
                boolean useCacheForAllThreads = PooledByteBufAllocator.defaultUseCacheForAllThreads();
                delegate = new PooledByteBufAllocator(
                    false,
                    nHeapArena,
                    0,
                    pageSize,
                    maxOrder,
                    smallCacheSize,
                    normalCacheSize,
                    useCacheForAllThreads
                );
                int chunkSizeInBytes = pageSize << maxOrder;
                ByteSizeValue chunkSize = ByteSizeValue.ofBytes(chunkSizeInBytes);
                SUGGESTED_MAX_ALLOCATION_SIZE = chunkSizeInBytes;
                DESCRIPTION = "[name=elasticsearch_configured, chunk_size="
                    + chunkSize
                    + ", suggested_max_allocation_size="
                    + ByteSizeValue.ofBytes(SUGGESTED_MAX_ALLOCATION_SIZE)
                    + ", factors={es.unsafe.use_netty_default_chunk_and_page_size="
                    + useDefaultChunkAndPageSize()
                    + ", g1gc_enabled="
                    + g1gcEnabled
                    + ", g1gc_region_size="
                    + g1gcRegionSize
                    + "}]";
            }
            allocator = new NoDirectBuffers(delegate);
        }
        if (Assertions.ENABLED) {
            ALLOCATOR = new TrashingByteBufAllocator(allocator);
        } else {
            ALLOCATOR = allocator;
        }

        RECYCLER = new Recycler<>() {
            @Override
            public Recycler.V<BytesRef> obtain() {
                ByteBuf byteBuf = ALLOCATOR.heapBuffer(PageCacheRecycler.BYTE_PAGE_SIZE, PageCacheRecycler.BYTE_PAGE_SIZE);
                assert byteBuf.hasArray();
                BytesRef bytesRef = new BytesRef(byteBuf.array(), byteBuf.arrayOffset(), byteBuf.capacity());
                return new Recycler.V<>() {
                    @Override
                    public BytesRef v() {
                        return bytesRef;
                    }

                    @Override
                    public boolean isRecycled() {
                        return true;
                    }

                    @Override
                    public void close() {
                        byteBuf.release();
                    }
                };
            }

            @Override
            public int pageSize() {
                return PageCacheRecycler.BYTE_PAGE_SIZE;
            }
        };
    }

    public static void logAllocatorDescriptionIfNeeded() {
        if (descriptionLogged.compareAndSet(false, true)) {
            logger.info("creating NettyAllocator with the following configs: " + NettyAllocator.getAllocatorDescription());
        }
    }

    public static ByteBufAllocator getAllocator() {
        return ALLOCATOR;
    }

    public static Recycler<BytesRef> getRecycler() {
        return RECYCLER;
    }

    public static long suggestedMaxAllocationSize() {
        return SUGGESTED_MAX_ALLOCATION_SIZE;
    }

    public static String getAllocatorDescription() {
        return DESCRIPTION;
    }

    public static Class<? extends Channel> getChannelType() {
        if (ALLOCATOR instanceof NoDirectBuffers) {
            return CopyBytesSocketChannel.class;
        } else {
            return Netty4NioSocketChannel.class;
        }
    }

    public static Class<? extends ServerChannel> getServerChannelType() {
        if (ALLOCATOR instanceof NoDirectBuffers) {
            return CopyBytesServerSocketChannel.class;
        } else {
            return NioServerSocketChannel.class;
        }
    }

    private static boolean useUnpooled(long heapSizeInBytes, boolean g1gcEnabled, boolean g1gcRegionSizeIsKnown, long g1RegionSize) {
        if (userForcedUnpooled()) {
            return true;
        } else if (userForcedPooled()) {
            return false;
        } else if (heapSizeInBytes <= 1 << 30) {
            // If the heap is 1GB or less we use unpooled
            return true;
        } else if (g1gcEnabled == false) {
            return false;
        } else {
            // If the G1GC is enabled and the region size is known and is less than 1MB we use unpooled.
            boolean g1gcRegionIsLessThan1MB = g1RegionSize < 1 << 20;
            return (g1gcRegionSizeIsKnown && g1gcRegionIsLessThan1MB);
        }
    }

    private static boolean userForcedUnpooled() {
        if (System.getProperty(USE_UNPOOLED) != null) {
            return Booleans.parseBoolean(System.getProperty(USE_UNPOOLED));
        } else {
            return false;
        }
    }

    private static boolean userForcedPooled() {
        if (System.getProperty(USE_UNPOOLED) != null) {
            return Booleans.parseBoolean(System.getProperty(USE_UNPOOLED)) == false;
        } else {
            return false;
        }
    }

    private static boolean useDefaultChunkAndPageSize() {
        if (System.getProperty(USE_NETTY_DEFAULT_CHUNK) != null) {
            return Booleans.parseBoolean(System.getProperty(USE_NETTY_DEFAULT_CHUNK));
        } else {
            return false;
        }
    }

    public static class NoDirectBuffers implements ByteBufAllocator {

        private final ByteBufAllocator delegate;

        private NoDirectBuffers(ByteBufAllocator delegate) {
            this.delegate = delegate;
        }

        @Override
        public ByteBuf buffer() {
            return heapBuffer();
        }

        @Override
        public ByteBuf buffer(int initialCapacity) {
            return heapBuffer(initialCapacity);
        }

        @Override
        public ByteBuf buffer(int initialCapacity, int maxCapacity) {
            return heapBuffer(initialCapacity, maxCapacity);
        }

        @Override
        public ByteBuf ioBuffer() {
            return heapBuffer();
        }

        @Override
        public ByteBuf ioBuffer(int initialCapacity) {
            return heapBuffer(initialCapacity);
        }

        @Override
        public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
            return heapBuffer(initialCapacity, maxCapacity);
        }

        @Override
        public ByteBuf heapBuffer() {
            return delegate.heapBuffer();
        }

        @Override
        public ByteBuf heapBuffer(int initialCapacity) {
            return delegate.heapBuffer(initialCapacity);
        }

        @Override
        public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
            return delegate.heapBuffer(initialCapacity, maxCapacity);
        }

        @Override
        public ByteBuf directBuffer() {
            throw new UnsupportedOperationException("Direct buffers not supported");
        }

        @Override
        public ByteBuf directBuffer(int initialCapacity) {
            throw new UnsupportedOperationException("Direct buffers not supported");
        }

        @Override
        public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
            throw new UnsupportedOperationException("Direct buffers not supported");
        }

        @Override
        public CompositeByteBuf compositeBuffer() {
            return compositeHeapBuffer();
        }

        @Override
        public CompositeByteBuf compositeBuffer(int maxNumComponents) {
            return compositeHeapBuffer(maxNumComponents);
        }

        @Override
        public CompositeByteBuf compositeHeapBuffer() {
            return delegate.compositeHeapBuffer();
        }

        @Override
        public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
            return delegate.compositeHeapBuffer(maxNumComponents);
        }

        @Override
        public CompositeByteBuf compositeDirectBuffer() {
            throw new UnsupportedOperationException("Direct buffers not supported.");
        }

        @Override
        public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
            throw new UnsupportedOperationException("Direct buffers not supported.");
        }

        @Override
        public boolean isDirectBufferPooled() {
            assert delegate.isDirectBufferPooled() == false;
            return false;
        }

        @Override
        public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
            return delegate.calculateNewCapacity(minNewCapacity, maxCapacity);
        }

        public ByteBufAllocator getDelegate() {
            return delegate;
        }
    }

    static class TrashingCompositeByteBuf extends CompositeByteBuf {

        TrashingCompositeByteBuf(ByteBufAllocator alloc, boolean direct, int maxNumComponents) {
            super(alloc, direct, maxNumComponents);
        }

        @Override
        protected void deallocate() {
            TrashingByteBufAllocator.trashBuffer(this);
            super.deallocate();
        }
    }

    static class TrashingByteBufAllocator extends NoDirectBuffers {

        static int DEFAULT_MAX_COMPONENTS = 16;

        static void trashBuffer(ByteBuf buf) {
            for (var nioBuf : buf.nioBuffers()) {
                if (nioBuf.hasArray()) {
                    var from = nioBuf.arrayOffset() + nioBuf.position();
                    var to = from + nioBuf.remaining();
                    Arrays.fill(nioBuf.array(), from, to, (byte) 0);
                }
            }
        }

        TrashingByteBufAllocator(ByteBufAllocator delegate) {
            super(delegate);
        }

        @Override
        public ByteBuf heapBuffer() {
            return new TrashingByteBuf(super.heapBuffer());
        }

        @Override
        public ByteBuf heapBuffer(int initialCapacity) {
            return new TrashingByteBuf(super.heapBuffer(initialCapacity));
        }

        @Override
        public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
            return new TrashingByteBuf(super.heapBuffer(initialCapacity, maxCapacity));
        }

        @Override
        public CompositeByteBuf compositeHeapBuffer() {
            return new TrashingCompositeByteBuf(this, false, DEFAULT_MAX_COMPONENTS);
        }

        @Override
        public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
            return new TrashingCompositeByteBuf(this, false, maxNumComponents);
        }

    }
}
