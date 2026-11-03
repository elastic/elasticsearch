/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal

import spock.lang.Specification

import com.sun.net.httpserver.HttpServer

import java.net.http.HttpClient
import java.util.zip.CRC32
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

/**
 * Tests {@link RemoteZipEntryReader} against an in-process HTTP server serving a zip built in the test.
 * The server honours {@code Range} requests unless {@code ignoreRanges} is set, which covers servers that
 * answer every request with the whole body.
 */
class RemoteZipEntryReaderSpec extends Specification {

    private HttpServer server
    private HttpClient httpClient = HttpClient.newHttpClient()
    private String url
    private List<String> requestedRanges = []
    private boolean ignoreRanges = false

    def cleanup() {
        server?.stop(0)
    }

    def "lists entry names from the central directory without downloading the archive"() {
        given:
        def padding = new byte[2 * 1024 * 1024]
        new Random(42).nextBytes(padding)
        serve(zip(["big.bin": padding, "dir/a.jar": "a".bytes, "dir/b.txt": "b".bytes]))

        when:
        def names = RemoteZipEntryReader.open(httpClient, url).entryNames()

        then:
        names == ["big.bin", "dir/a.jar", "dir/b.txt"]
        requestedRanges.every { it != null }
    }

    def "reads a deflated entry"() {
        given:
        def content = ("x-pack-sql-jdbc " * 1000).bytes
        serve(zip(["org/x/x-pack-sql-jdbc-9.4.10-20261103.085400-1.jar": content], ZipEntry.DEFLATED))

        expect:
        RemoteZipEntryReader.open(httpClient, url).read("org/x/x-pack-sql-jdbc-9.4.10-20261103.085400-1.jar") == content
    }

    def "reads a stored entry"() {
        given:
        def content = "stored".bytes
        serve(zip(["a/stored.jar": content], ZipEntry.STORED))

        expect:
        RemoteZipEntryReader.open(httpClient, url).read("a/stored.jar") == content
    }

    def "works against a server that ignores range requests"() {
        given:
        ignoreRanges = true
        serve(zip(["a.jar": "a".bytes, "b.jar": "bee".bytes]))

        expect:
        RemoteZipEntryReader.open(httpClient, url).read("b.jar") == "bee".bytes
    }

    def "reading a missing entry names the entry and the archive"() {
        given:
        serve(zip(["a.jar": "a".bytes]))

        when:
        RemoteZipEntryReader.open(httpClient, url).read("missing.jar")

        then:
        def e = thrown(IOException)
        e.message.contains("missing.jar")
        e.message.contains(url)
    }

    def "a missing archive fails with the HTTP status"() {
        given:
        serve(null)

        when:
        RemoteZipEntryReader.open(httpClient, url)

        then:
        def e = thrown(IOException)
        e.message.contains("404")
        e.message.contains(url)
    }

    private void serve(byte[] body) {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0)
        server.createContext("/archive.zip") { exchange ->
            if (body == null) {
                exchange.sendResponseHeaders(404, -1)
                exchange.close()
                return
            }
            def range = exchange.requestHeaders.getFirst("Range")
            if (exchange.requestMethod == "HEAD") {
                exchange.responseHeaders.add("Content-Length", String.valueOf(body.length))
                exchange.sendResponseHeaders(200, -1)
            } else if (range != null && ignoreRanges == false) {
                requestedRanges << range
                def (start, end) = range.substring("bytes=".length()).split("-").collect { Long.parseLong(it) }
                def slice = Arrays.copyOfRange(body, (int) start, (int) Math.min(end + 1, body.length))
                exchange.responseHeaders.add("Content-Range", "bytes ${start}-${start + slice.length - 1}/${body.length}")
                exchange.sendResponseHeaders(206, slice.length)
                exchange.responseBody.withStream { it.write(slice) }
            } else {
                requestedRanges << null
                exchange.sendResponseHeaders(200, body.length)
                exchange.responseBody.withStream { it.write(body) }
            }
            exchange.close()
        }
        server.start()
        url = "http://${server.address.address.hostAddress}:${server.address.port}/archive.zip"
    }

    private static byte[] zip(Map<String, byte[]> entries, int method = ZipEntry.DEFLATED) {
        def baos = new ByteArrayOutputStream()
        new ZipOutputStream(baos).withCloseable { zos ->
            entries.each { name, content ->
                def entry = new ZipEntry(name)
                entry.method = method
                if (method == ZipEntry.STORED) {
                    def crc = new CRC32()
                    crc.update(content)
                    entry.size = content.length
                    entry.compressedSize = content.length
                    entry.crc = crc.value
                }
                zos.putNextEntry(entry)
                zos.write(content)
                zos.closeEntry()
            }
        }
        return baos.toByteArray()
    }
}
