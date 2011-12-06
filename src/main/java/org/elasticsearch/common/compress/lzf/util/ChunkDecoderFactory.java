package org.elasticsearch.common.compress.lzf.util;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.compress.lzf.ChunkDecoder;
import org.elasticsearch.common.compress.lzf.impl.UnsafeChunkDecoder;
import org.elasticsearch.common.compress.lzf.impl.VanillaChunkDecoder;

/**
 * Simple helper class used for loading
 * {@link ChunkDecoder} implementations, based on criteria
 * such as "fastest available".
 * <p/>
 * Yes, it looks butt-ugly, but does the job. Nonetheless, if anyone
 * has lipstick for this pig, let me know.
 *
 * @since 0.9
 */
public class ChunkDecoderFactory {
    private final static ChunkDecoderFactory _instance;

    static {
        Class<?> impl = null;
        try {
            // first, try loading optimal one, which uses Sun JDK Unsafe...
            impl = (Class<?>) Class.forName(UnsafeChunkDecoder.class.getName());
        } catch (Throwable t) {
        }
        if (impl == null) {
            impl = VanillaChunkDecoder.class;
        }
        // ES: Seems like: https://github.com/ning/compress/issues/13, is fixed, so enable by defualt, but only from 0.19
        if (!Booleans.parseBoolean(System.getProperty("compress.lzf.decoder.optimized"), true)) {
            impl = VanillaChunkDecoder.class;
        }
        _instance = new ChunkDecoderFactory(impl);
    }

    private final Class<? extends ChunkDecoder> _implClass;

    @SuppressWarnings("unchecked")
    private ChunkDecoderFactory(Class<?> imp) {
        _implClass = (Class<? extends ChunkDecoder>) imp;
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Public API
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * Method to use for getting decompressor instance that uses the most optimal
     * available methods for underlying data access. It should be safe to call
     * this method as implementations are dynamically loaded; however, on some
     * non-standard platforms it may be necessary to either directly load
     * instances, or use {@link #safeInstance()}.
     */
    public static ChunkDecoder optimalInstance() {
        try {
            return _instance._implClass.newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load a ChunkDecoder instance (" + e.getClass().getName() + "): "
                    + e.getMessage(), e);
        }
    }

    /**
     * Method that can be used to ensure that a "safe" decompressor instance is loaded.
     * Safe here means that it should work on any and all Java platforms.
     */
    public static ChunkDecoder safeInstance() {
        // this will always succeed loading; no need to use dynamic class loading or instantiation
        return new VanillaChunkDecoder();
    }
}
