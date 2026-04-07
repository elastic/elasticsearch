/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.util

import spock.lang.Specification
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

class HttpUtilsSpec extends Specification {

    def "http utils retries once and eventually succeeds"() {
        given:
        AtomicInteger requests = new AtomicInteger()
        byte[] body = "ok".getBytes(StandardCharsets.UTF_8)

        HttpServer server = startServer { exchange ->
            int request = requests.getAndIncrement()
            if (request == 0) {
                exchange.sendResponseHeaders(500, 0)
                exchange.getResponseBody().withStream { it.write("fail".getBytes(StandardCharsets.UTF_8)) }
            } else {
                exchange.sendResponseHeaders(200, body.length)
                exchange.getResponseBody().withStream { it.write(body) }
            }
            exchange.close()
        }

        String host = server.getAddress().getAddress().getHostAddress()
        String url = new URI("http", null, host, server.getAddress().getPort(), "/branches.json", null, null).toString()
        List<Long> backoffs = []
        HttpUtils.Sleeper sleeper = { backoffs << it }

        when:
        byte[] bytes = HttpUtils.readHttpBytesWithRetry(url, 3, 10, sleeper)

        then:
        bytes == body
        requests.get() == 2
        backoffs == [10L]

        cleanup:
        server?.stop(0)
    }

    def "http utils exhausts retries"() {
        given:
        AtomicInteger requests = new AtomicInteger()

        HttpServer server = startServer { exchange ->
            requests.incrementAndGet()
            exchange.sendResponseHeaders(500, 0)
            exchange.getResponseBody().withStream { it.write("fail".getBytes(StandardCharsets.UTF_8)) }
            exchange.close()
        }

        String host = server.getAddress().getAddress().getHostAddress()
        String url = new URI("http", null, host, server.getAddress().getPort(), "/branches.json", null, null).toString()
        List<Long> backoffs = []
        HttpUtils.Sleeper sleeper = { backoffs << it }

        when:
        HttpUtils.readHttpBytesWithRetry(url, 3, 10, sleeper)

        then:
        thrown(IOException)
        requests.get() == 3
        backoffs == [10L, 20L]

        cleanup:
        server?.stop(0)
    }

    private static HttpServer startServer(HttpHandler handler) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0)
        server.createContext("/branches.json", handler)
        server.start()
        return server
    }

}
