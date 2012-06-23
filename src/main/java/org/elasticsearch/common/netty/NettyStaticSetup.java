package org.elasticsearch.common.netty;

import org.elasticsearch.transport.netty.NettyInternalESLoggerFactory;
import org.jboss.netty.logging.InternalLogger;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;

/**
 */
public class NettyStaticSetup {

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
