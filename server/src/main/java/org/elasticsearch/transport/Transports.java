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

package org.elasticsearch.transport;

import org.elasticsearch.http.HttpServerTransport;

import java.util.Arrays;

public enum Transports {
    ;

    /** threads whose name is prefixed by this string will be considered network threads, even though they aren't */
    public static final String TEST_MOCK_TRANSPORT_THREAD_PREFIX = "__mock_network_thread";

    public static final String NIO_TRANSPORT_WORKER_THREAD_NAME_PREFIX = "es_nio_transport_worker";
    public static final String NIO_TRANSPORT_ACCEPTOR_THREAD_NAME_PREFIX = "es_nio_transport_acceptor";

    /**
     * Utility method to detect whether a thread is a network thread. Typically
     * used in assertions to make sure that we do not call blocking code from
     * networking threads.
     */
    public static final boolean isTransportThread(Thread t) {
        final String threadName = t.getName();
        for (String s : Arrays.asList(
                HttpServerTransport.HTTP_SERVER_WORKER_THREAD_NAME_PREFIX,
                TcpTransport.TRANSPORT_SERVER_WORKER_THREAD_NAME_PREFIX,
                TcpTransport.TRANSPORT_CLIENT_BOSS_THREAD_NAME_PREFIX,
                TEST_MOCK_TRANSPORT_THREAD_PREFIX,
                NIO_TRANSPORT_WORKER_THREAD_NAME_PREFIX,
                NIO_TRANSPORT_ACCEPTOR_THREAD_NAME_PREFIX)) {
            if (threadName.contains(s)) {
                return true;
            }
        }
        return false;
    }

    public static boolean assertTransportThread() {
        final Thread t = Thread.currentThread();
        assert isTransportThread(t) : "Expected transport thread but got [" + t + "]";
        return true;
    }

    public static boolean assertNotTransportThread(String reason) {
        final Thread t = Thread.currentThread();
        assert isTransportThread(t) == false : "Expected current thread [" + t + "] to not be a transport thread. Reason: [" + reason + "]";
        return true;
    }
}
