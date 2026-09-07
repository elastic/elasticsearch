/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.nativelibs

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import org.gradle.api.GradleException
import spock.lang.Specification


/**
 * Exercised against a real HTTP server rather than a stub: the behaviour worth pinning is how the
 * repository reacts to actual status codes and bodies, which a hand-written fake would only echo back.
 */
class NativeArtifactRepositorySpec extends Specification {

    static final String NAME = "vec"
    static final String HASH = "abc123"
    static final byte[] CONTENT = "zip-bytes".getBytes("UTF-8")

    HttpServer server

    def cleanup() {
        server?.stop(0)
    }

    def "download returns the artifact when it is published"() {
        given:
        def repository = repositoryServing { exchange ->
            respond(exchange, 200, CONTENT)
        }

        expect:
        repository.download(NAME, HASH).get() == CONTENT
    }

    def "download reports absence for a hash that was never published, not a failure"() {
        given:
        def repository = repositoryServing { exchange ->
            respond(exchange, 404, new byte[0])
        }

        expect:
        repository.download(NAME, HASH).isEmpty()
    }

    def "download fails loudly on a server error"() {
        given:
        def repository = repositoryServing { exchange ->
            respond(exchange, 500, "boom".getBytes("UTF-8"))
        }

        when:
        repository.download(NAME, HASH)

        then:
        def e = thrown(Exception)
        e.message.contains("500") || e.cause?.message?.contains("500")
    }

    def "publish uploads the content with the credential"() {
        given:
        def received = new ByteArrayOutputStream()
        def apiKeys = []
        def repository = repositoryServing { exchange ->
            if (exchange.requestMethod == "PUT") {
                apiKeys << exchange.requestHeaders.getFirst("X-JFrog-Art-Api")
                received.write(exchange.requestBody.bytes)
                respond(exchange, 201, new byte[0])
            } else {
                respond(exchange, 200, CONTENT)
            }
        }

        when:
        repository.publish(NAME, HASH, CONTENT, "secret-key")

        then:
        received.toByteArray() == CONTENT
        apiKeys == ["secret-key"]
    }

    def "publish succeeds when the hash already holds the same content"() {
        given:
        def repository = repositoryServing { exchange ->
            if (exchange.requestMethod == "PUT") {
                exchange.requestBody.bytes
                respond(exchange, 403, new byte[0])
            } else {
                respond(exchange, 200, CONTENT)
            }
        }

        when:
        repository.publish(NAME, HASH, CONTENT, "secret-key")

        then:
        noExceptionThrown()
    }

    def "publish fails when the upload is rejected and the hash holds something else"() {
        given:
        def repository = repositoryServing { exchange ->
            if (exchange.requestMethod == "PUT") {
                exchange.requestBody.bytes
                respond(exchange, 403, new byte[0])
            } else {
                respond(exchange, 200, "different".getBytes("UTF-8"))
            }
        }

        when:
        repository.publish(NAME, HASH, CONTENT, "secret-key")

        then:
        def e = thrown(GradleException)
        e.message.contains("403")
    }

    def "publish fails when the upload is rejected and nothing is published"() {
        given:
        def repository = repositoryServing { exchange ->
            if (exchange.requestMethod == "PUT") {
                exchange.requestBody.bytes
                respond(exchange, 403, new byte[0])
            } else {
                respond(exchange, 404, new byte[0])
            }
        }

        when:
        repository.publish(NAME, HASH, CONTENT, "secret-key")

        then:
        thrown(GradleException)
    }

    def "verifyPublished rejects a truncated artifact"() {
        given:
        def repository = repositoryServing { exchange ->
            respond(exchange, 200, "zip-by".getBytes("UTF-8"))
        }

        when:
        repository.verifyPublished(NAME, HASH, CONTENT)

        then:
        def e = thrown(GradleException)
        e.message.contains("does not match")
        e.message.contains("${CONTENT.length} bytes sent")
    }

    def "verifyPublished rejects an artifact that vanished"() {
        given:
        def repository = repositoryServing { exchange ->
            respond(exchange, 404, new byte[0])
        }

        when:
        repository.verifyPublished(NAME, HASH, CONTENT)

        then:
        def e = thrown(GradleException)
        e.message.contains("cannot be read back")
    }

    def "verifyPublished accepts a matching artifact"() {
        given:
        def repository = repositoryServing { exchange ->
            respond(exchange, 200, CONTENT)
        }

        when:
        repository.verifyPublished(NAME, HASH, CONTENT)

        then:
        noExceptionThrown()
    }

    private NativeArtifactRepository repositoryServing(HttpHandler handler) {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0)
        server.createContext("/", handler)
        server.start()
        String host = server.address.address.hostAddress
        return new NativeArtifactRepository("http://${host}:${server.address.port}")
    }

    private static void respond(HttpExchange exchange, int status, byte[] body) {
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length)
        if (body.length > 0) {
            exchange.responseBody.withStream { it.write(body) }
        }
        exchange.close()
    }
}
