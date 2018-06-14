package org.elasticsearch.http.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledHeapByteBuf;
import org.elasticsearch.nio.InboundChannelBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PageByteBuf extends UnpooledHeapByteBuf {

    private final Runnable releasable;

    private PageByteBuf(byte[] array, Runnable releasable) {
        super(UnpooledByteBufAllocator.DEFAULT, array, array.length);
        this.releasable = releasable;
    }

    public static ByteBuf byteBufFromPages(InboundChannelBuffer.Page[] pages) {
        int componentCount = pages.length;
        if (componentCount == 1) {
            return byteBufFromPage(pages[0]);
        } else {
            int maxComponents = Math.max(16, componentCount);
            final List<ByteBuf> components = new ArrayList<>(componentCount);
            for (InboundChannelBuffer.Page page: pages) {
                components.add(byteBufFromPage(page));
            }
            return new CompositeByteBuf(UnpooledByteBufAllocator.DEFAULT, false, maxComponents, components);
        }
    }

    public static ByteBuf byteBufFromPage(InboundChannelBuffer.Page page) {
        ByteBuffer buffer = page.getByteBuffer();
        assert !buffer.isDirect() && buffer.hasArray() : "Must be a heap buffer with an array";
        int offset = buffer.arrayOffset() + buffer.position();
        PageByteBuf newByteBuf = new PageByteBuf(buffer.array(), page::close);
        return newByteBuf.slice(offset, buffer.remaining());
    }


    @Override
    protected void deallocate() {
        try {
            super.deallocate();
        } finally {
            releasable.run();
        }
    }
}
