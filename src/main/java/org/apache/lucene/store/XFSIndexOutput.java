package org.apache.lucene.store;

import org.elasticsearch.common.RateLimiter;

import java.io.IOException;

/**
 */
class XFSIndexOutput extends FSDirectory.FSIndexOutput {

    private final RateLimiter rateLimiter;

    private final StoreRateLimiting.Listener rateListener;

    XFSIndexOutput(FSDirectory parent, String name, RateLimiter rateLimiter, StoreRateLimiting.Listener rateListener) throws IOException {
        super(parent, name);
        this.rateLimiter = rateLimiter;
        this.rateListener = rateListener;
    }

    @Override
    public void flushBuffer(byte[] b, int offset, int size) throws IOException {
        rateListener.onPause(rateLimiter.pause(size));
        super.flushBuffer(b, offset, size);
    }
}
