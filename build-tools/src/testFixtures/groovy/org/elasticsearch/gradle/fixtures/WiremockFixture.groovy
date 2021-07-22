/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.fixtures

import com.github.tomakehurst.wiremock.WireMockServer
import org.gradle.testkit.runner.BuildResult

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse
import static com.github.tomakehurst.wiremock.client.WireMock.get
import static com.github.tomakehurst.wiremock.client.WireMock.head
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo

/**
 * A test fixture that allows running testkit builds with wiremock
 * */
class WiremockFixture {

    /**
     * the buildRunClosure has passed an instance of WireMockServer that can be used to access e.g. the baseUrl of
     * the configured server:
     *
     * <pre>
     *  WiremockFixture.withWireMock(mockRepoUrl, mockedContent) { server ->
     *      buildFile << """
     *          // wire a gradle repository with wiremock
*               repositories {
     *              maven {
     *                 url = '${server.baseUrl()}'
     *              }
     *          }
     *      }
     *      gadleRunner('myTask').build()
     * </pre>
     * */
    static BuildResult withWireMock(String expectedUrl, byte[] expectedContent, Closure<BuildResult> buildRunClosure) {
        WireMockServer wireMock = new WireMockServer(0);
        try {
            wireMock.stubFor(head(urlEqualTo(expectedUrl)).willReturn(aResponse().withStatus(200)));
            wireMock.stubFor(
                    get(urlEqualTo(expectedUrl)).willReturn(aResponse().withStatus(200).withBody(expectedContent))
            )
            wireMock.start();
            return buildRunClosure.call(wireMock);
        } catch (Exception e) {
            // for debugging
            System.err.println("missed requests: " + wireMock.findUnmatchedRequests().getRequests());
            throw e;
        } finally {
            wireMock.stop();
        }
    }

}
