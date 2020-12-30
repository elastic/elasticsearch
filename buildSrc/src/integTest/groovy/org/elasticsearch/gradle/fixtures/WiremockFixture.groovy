/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
