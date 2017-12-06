package org.elasticsearch.transport.nio;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;

import java.nio.ByteBuffer;

public class ByteBufferProvider {

    public static final ByteBufferProvider NON_RECYCLING_INSTANCE = new ByteBufferProvider();

    private final PageCacheRecycler pageCacheRecycler;

    private ByteBufferProvider() {
        this(null);
    }

    public ByteBufferProvider(PageCacheRecycler pageCacheRecycler) {
        this.pageCacheRecycler = pageCacheRecycler;
    }

    public ReleasableByteBuffer getByteBufferPage() {
        if (pageCacheRecycler != null) {
            Recycler.V<byte[]> bytePage = pageCacheRecycler.bytePage(false);
            return new ReleasableByteBuffer(ByteBuffer.wrap(bytePage.v()), bytePage);
        } else {
            return new ReleasableByteBuffer(ByteBuffer.allocate(BigArrays.BYTE_PAGE_SIZE), () -> {});
        }
    }

    static class ReleasableByteBuffer implements Releasable {

        private final ByteBuffer byteBuffer;
        private final Releasable releasable;

        private ReleasableByteBuffer(ByteBuffer byteBuffer, Releasable releasable) {
            this.byteBuffer = byteBuffer;
            this.releasable = releasable;
        }

        ByteBuffer byteBuffer() {
            return byteBuffer;
        }

        @Override
        public void close() {
            releasable.close();
        }
    }
}
