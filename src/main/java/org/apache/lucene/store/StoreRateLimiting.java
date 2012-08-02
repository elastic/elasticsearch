package org.apache.lucene.store;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.RateLimiter;
import org.elasticsearch.common.unit.ByteSizeValue;

/**
 */
public class StoreRateLimiting {

    public static interface Provider {

        StoreRateLimiting rateLimiting();
    }

    public interface Listener {

        void onPause(long nanos);
    }

    public static enum Type {
        NONE,
        MERGE,
        ALL;

        public static Type fromString(String type) throws ElasticSearchIllegalArgumentException {
            if ("none".equalsIgnoreCase(type)) {
                return NONE;
            } else if ("merge".equalsIgnoreCase(type)) {
                return MERGE;
            } else if ("all".equalsIgnoreCase(type)) {
                return ALL;
            }
            throw new ElasticSearchIllegalArgumentException("rate limiting type [" + type + "] not valid, can be one of [all|merge|none]");
        }
    }

    private final RateLimiter rateLimiter = new RateLimiter(0);
    private volatile RateLimiter actualRateLimiter;

    private volatile Type type;

    public StoreRateLimiting() {

    }

    @Nullable
    public RateLimiter getRateLimiter() {
        return actualRateLimiter;
    }

    public void setMaxRate(ByteSizeValue rate) {
        if (rate.bytes() <= 0) {
            actualRateLimiter = null;
        } else if (actualRateLimiter == null) {
            actualRateLimiter = rateLimiter;
            actualRateLimiter.setMaxRate(rate.mbFrac());
        } else {
            assert rateLimiter == actualRateLimiter;
            rateLimiter.setMaxRate(rate.mbFrac());
        }
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public void setType(String type) throws ElasticSearchIllegalArgumentException {
        this.type = Type.fromString(type);
    }
}
