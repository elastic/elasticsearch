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

package org.elasticsearch.gradle;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.elasticsearch.gradle.test.GradleIntegrationTestCase;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.head;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.hamcrest.CoreMatchers.equalTo;

public class JdkDownloadPluginIT extends GradleIntegrationTestCase {

    private static final String FAKE_JDK_VERSION = "1.0.2+99";
    private static final Pattern JDK_HOME_LOGLINE = Pattern.compile("JDK HOME: (.*)");
    private static final Pattern NUM_CONFIGS_LOGLINE = Pattern.compile("NUM CONFIGS: (.*)");

    public void testLinuxExtraction() throws IOException {
        assertExtraction("getLinuxJdk", "linux", "bin/java");
    }

    public void testDarwinExtraction() throws IOException {
        assertExtraction("getDarwinJdk", "osx", "Contents/Home/bin/java");
    }

    public void testWindowsExtraction() throws IOException {
        assertExtraction("getWindowsJdk", "windows", "bin/java");
    }

    public void testCrossProjectReuse() throws IOException {
        runBuild("numConfigurations", "linux", result -> {
            Matcher matcher = NUM_CONFIGS_LOGLINE.matcher(result.getOutput());
            assertTrue("could not find num configs in output: " + result.getOutput(), matcher.find());
            assertThat(Integer.parseInt(matcher.group(1)), equalTo(6)); // 3 import configs, 3 export configs
        });
    }

    public void assertExtraction(String taskname, String platform, String javaBin) throws IOException {
        runBuild(taskname, platform, result -> {
            Matcher matcher = JDK_HOME_LOGLINE.matcher(result.getOutput());
            assertTrue("could not find jdk home in output: " + result.getOutput(), matcher.find());
            String jdkHome = matcher.group(1);
            Path javaPath = Paths.get(jdkHome, javaBin);
            assertTrue(javaPath.toString(), Files.exists(javaPath));
        });
    }

    private void runBuild(String taskname, String platform, Consumer<BuildResult> assertions) throws IOException {
        WireMockServer wireMock = new WireMockServer(0);
        try {
            String extension = platform.equals("windows") ? "zip" : "tar.gz";
            String filename = "openjdk-1.0.2_" + platform + "-x64_bin." + extension;
            wireMock.stubFor(head(urlEqualTo("/java/GA/jdk1/99/GPL/" + filename))
                .willReturn(aResponse().withStatus(200)));
            final byte[] filebytes;
            try (InputStream stream = JdkDownloadPluginIT.class.getResourceAsStream(filename)) {
                filebytes = stream.readAllBytes();
            }
            wireMock.stubFor(get(urlEqualTo("/java/GA/jdk1/99/GPL/" + filename))
                .willReturn(aResponse().withStatus(200).withBody(filebytes)));
            wireMock.start();

            GradleRunner runner = GradleRunner.create().withProjectDir(getProjectDir("jdk-download"))
                .withArguments(taskname,
                    "-Dlocal.repo.path=" + getLocalTestRepoPath(),
                    "-Dtests.jdk_version=" + FAKE_JDK_VERSION,
                    "-Dtests.jdk_repo=" + wireMock.baseUrl())
                .withPluginClasspath();

            BuildResult result = runner.build();
            assertions.accept(result);
        } catch (Exception e) {
            // for debugging
            System.err.println("missed requests: " + wireMock.findUnmatchedRequests().getRequests());
            throw e;
        } finally {
            wireMock.stop();
        }
    }
}
