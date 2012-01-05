package org.elasticsearch.common.cache;

import com.google.common.cache.CacheBuilder;

import java.lang.reflect.Method;

/**
 */
public class CacheBuilderHelper {

    private static final Method cacheBuilderDisableStatsMethod;

    static {
        Method cacheBuilderDisableStatsMethodX = null;
        try {
            cacheBuilderDisableStatsMethodX = CacheBuilder.class.getDeclaredMethod("disableStats");
            cacheBuilderDisableStatsMethodX.setAccessible(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        cacheBuilderDisableStatsMethod = cacheBuilderDisableStatsMethodX;
    }

    public static void disableStats(CacheBuilder cacheBuilder) {
        if (cacheBuilderDisableStatsMethod != null) {
            try {
                cacheBuilderDisableStatsMethod.invoke(cacheBuilder);
            } catch (Exception e) {
                e.printStackTrace();
                // ignore
            }
        }
    }
}
