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
