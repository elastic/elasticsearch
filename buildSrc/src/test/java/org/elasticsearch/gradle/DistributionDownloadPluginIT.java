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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.head;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

public class DistributionDownloadPluginIT extends GradleIntegrationTestCase {
    // TODO: check reuse of root task across projects MOVE TO UNIT TEST
    // TODO: make extracted assert also do file assert, to reduce separate test builds
    // TODO: future: check integ-test-zip to maven, snapshots to snapshot service for external project

    private static final Pattern DISTRO_FILE_LOGLINE = Pattern.compile("DISTRO FILE: (.*)");
    private static final Pattern DISTRO_EXTRACTED_LOGLINE = Pattern.compile("DISTRO EXTRACTED: (.*)");

    public void testCurrent() throws Exception {
        String projectName = ":distribution:archives:linux-tar";
        assertFileDistro(VersionProperties.getElasticsearch(), "archive", "linux", null, null,
            "tests.local_distro.config", "default",
            "tests.local_distro.project", projectName);
        assertExtractedDistro(VersionProperties.getElasticsearch(), "archive", "linux", null, null,
            "tests.local_distro.config", "default",
            "tests.local_distro.project", projectName);
    }

    public void testBwc() throws Exception {
        assertFileDistro("1.1.0", "archive", "linux", null, null,
            "tests.local_distro.config", "linux-tar",
            "tests.local_distro.project", ":distribution:bwc:minor",
            "tests.current_version", "2.0.0");
        assertExtractedDistro("1.1.0", "archive", "linux", null, null,
            "tests.local_distro.config", "linux-tar",
            "tests.local_distro.project", ":distribution:bwc:minor",
            "tests.current_version", "2.0.0");
    }

    public void testReleased() throws Exception {
        WireMockServer wireMock = new WireMockServer(0);
        try {
            final byte[] filebytes;
            try (InputStream stream =
                     Files.newInputStream(Paths.get("src/testKit/distribution-download/distribution/files/fake_elasticsearch.zip"))) {
                filebytes = stream.readAllBytes();
            }
            String urlPath = "/downloads/elasticsearch/elasticsearch-1.0.0-windows-x86_64.zip";
            wireMock.stubFor(head(urlEqualTo(urlPath)).willReturn(aResponse().withStatus(200)));
            wireMock.stubFor(get(urlEqualTo(urlPath)).willReturn(aResponse().withStatus(200).withBody(filebytes)));
            wireMock.start();

            assertFileDistro("1.0.0", "archive", "windows", null, null,
                "tests.download_service", wireMock.baseUrl());
            assertExtractedDistro("1.0.0", "archive", "windows", null, null,
                "tests.download_service", wireMock.baseUrl());
        } catch (Exception e) {
            // for debugging
            System.err.println("missed requests: " + wireMock.findUnmatchedRequests().getRequests());
            throw e;
        } finally {
            wireMock.stop();
        }
    }

    private void assertFileDistro(String version, String type, String platform, String flavor, Boolean bundledJdk,
                                  String... sysProps) throws IOException {
        List<String> finalSysProps = new ArrayList<>();
        addDistroSysProps(finalSysProps, version, type, platform, flavor, bundledJdk);
        finalSysProps.addAll(Arrays.asList(sysProps));
        runBuild(":subproj:getDistro", result -> {
                Matcher matcher = DISTRO_FILE_LOGLINE.matcher(result.getOutput());
                assertTrue("could not find distro file in output: " + result.getOutput(), matcher.find());
                String distroFile = matcher.group(1);
                assertTrue(distroFile, Files.exists(Paths.get(distroFile)));
            }, finalSysProps.toArray(new String[0]));
    }

    private void assertExtractedDistro(String version, String type, String platform, String flavor, Boolean bundledJdk,
                                       String... sysProps) throws IOException {
        List<String> finalSysProps = new ArrayList<>();
        addDistroSysProps(finalSysProps, version, type, platform, flavor, bundledJdk);
        finalSysProps.addAll(Arrays.asList(sysProps));
        runBuild(":subproj:getDistroExtracted", result -> {
            Matcher matcher = DISTRO_EXTRACTED_LOGLINE.matcher(result.getOutput());
            assertTrue("could not find distro extracted in output: " + result.getOutput(), matcher.find());
            String distroDir = matcher.group(1);
            assertTrue(distroDir, Files.isDirectory(Paths.get(distroDir)));
        }, finalSysProps.toArray(new String[0]));
    }

    private void runBuild(String taskname, Consumer<BuildResult> assertions, String... sysProps) throws IOException {
        assert sysProps.length % 2 == 0;
        List<String> args = new ArrayList<>();
        args.add(taskname);
        args.add("-Dlocal.repo.path=" + getLocalTestRepoPath());
        for (int i = 0; i < sysProps.length; i += 2) {
            args.add("-D" + sysProps[i] + "=" + sysProps[i + 1]);
        }
        args.add("-s");
        GradleRunner runner = getGradleRunner("distribution-download").withArguments(args);

        BuildResult result = runner.build();
        assertions.accept(result);
    }

    private void addDistroSysProps(List<String> sysProps, String version, String type, String platform, String flavor, Boolean bundledJdk) {
        if (version != null) {
            sysProps.add("tests.distro.version");
            sysProps.add(version);
        }
        if (type != null) {
            sysProps.add("tests.distro.type");
            sysProps.add(type);
        }
        if (platform != null) {
            sysProps.add("tests.distro.platform");
            sysProps.add(platform);
        }
        if (flavor != null) {
            sysProps.add("tests.distro.flavor");
            sysProps.add(flavor);
        }
        if (bundledJdk != null) {
            sysProps.add("tests.distro.bundledJdk");
            sysProps.add(bundledJdk.toString());
        }
    }
}
