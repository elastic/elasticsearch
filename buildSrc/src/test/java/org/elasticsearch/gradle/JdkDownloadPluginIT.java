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

public abstract class JdkDownloadPluginIT extends GradleIntegrationTestCase {

    private static final Pattern JDK_HOME_LOGLINE = Pattern.compile("JDK HOME: (.*)");
    private static final Pattern NUM_CONFIGS_LOGLINE = Pattern.compile("NUM CONFIGS: (.*)");

    protected abstract String oldJdkVersion();

    protected abstract String jdkVersion();

    protected abstract String jdkVendor();

    public final void testLinuxExtraction() throws IOException {
        assertExtraction("getLinuxJdk", "linux", "bin/java", jdkVendor(), jdkVersion());
    }

    public final void testDarwinExtraction() throws IOException {
        assertExtraction("getDarwinJdk", "osx", "Contents/Home/bin/java", jdkVendor(), jdkVersion());
    }

    public final void testWindowsExtraction() throws IOException {
        assertExtraction("getWindowsJdk", "windows", "bin/java", jdkVendor(), jdkVersion());
    }

    public final void testLinuxExtractionOldVersion() throws IOException {
        assertExtraction("getLinuxJdk", "linux", "bin/java", jdkVendor(), oldJdkVersion());
    }

    public final void testDarwinExtractionOldVersion() throws IOException {
        assertExtraction("getDarwinJdk", "osx", "Contents/Home/bin/java", jdkVendor(), oldJdkVersion());
    }

    public final void testWindowsExtractionOldVersion() throws IOException {
        assertExtraction("getWindowsJdk", "windows", "bin/java", jdkVendor(), oldJdkVersion());
    }

    public final void testCrossProjectReuse() throws IOException {
        runBuild("numConfigurations", "linux", result -> {
            Matcher matcher = NUM_CONFIGS_LOGLINE.matcher(result.getOutput());
            assertTrue("could not find num configs in output: " + result.getOutput(), matcher.find());
            assertThat(Integer.parseInt(matcher.group(1)), equalTo(6)); // 3 import configs, 3 export configs
        }, jdkVendor(), jdkVersion());
    }

    private void assertExtraction(String taskname, String platform, String javaBin, String vendor, String version) throws IOException {
        runBuild(taskname, platform, result -> {
            Matcher matcher = JDK_HOME_LOGLINE.matcher(result.getOutput());
            assertTrue("could not find jdk home in output: " + result.getOutput(), matcher.find());
            String jdkHome = matcher.group(1);
            Path javaPath = Paths.get(jdkHome, javaBin);
            assertTrue(javaPath.toString(), Files.exists(javaPath));
        }, vendor, version);
    }

    protected abstract String urlPath(boolean isOld, String platform, String extension);

    protected abstract byte[] filebytes(String platform, String extension) throws IOException;

    private void runBuild(String taskname, String platform, Consumer<BuildResult> assertions, String vendor, String version)
        throws IOException {
        WireMockServer wireMock = new WireMockServer(0);
        try {
            String extension = platform.equals("windows") ? "zip" : "tar.gz";
            boolean isOld = version.equals(oldJdkVersion());

            wireMock.stubFor(head(urlEqualTo(urlPath(isOld, platform, extension))).willReturn(aResponse().withStatus(200)));
            wireMock.stubFor(
                get(urlEqualTo(urlPath(isOld, platform, extension))).willReturn(
                    aResponse().withStatus(200).withBody(filebytes(platform, extension))
                )
            );
            wireMock.start();

            GradleRunner runner = GradleRunner.create()
                .withProjectDir(getProjectDir("jdk-download"))
                .withArguments(
                    taskname,
                    "-Dtests.jdk_vendor=" + vendor,
                    "-Dtests.jdk_version=" + version,
                    "-Dtests.jdk_repo=" + wireMock.baseUrl(),
                    "-i"
                )
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
