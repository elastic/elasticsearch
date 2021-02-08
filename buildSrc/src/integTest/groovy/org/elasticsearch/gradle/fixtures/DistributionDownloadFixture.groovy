/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.fixtures

import org.elasticsearch.gradle.Architecture
import org.elasticsearch.gradle.ElasticsearchDistribution
import org.elasticsearch.gradle.VersionProperties
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner

class DistributionDownloadFixture {

    public static final String INIT_SCRIPT = "repositories-init.gradle"

    static BuildResult withMockedDistributionDownload(GradleRunner gradleRunner, Closure<BuildResult> buildRunClosure) {
        return withMockedDistributionDownload(VersionProperties.getElasticsearch(), ElasticsearchDistribution.CURRENT_PLATFORM,
                gradleRunner, buildRunClosure)
    }

    static BuildResult withMockedDistributionDownload(String version, ElasticsearchDistribution.Platform platform,
                                                      GradleRunner gradleRunner, Closure<BuildResult> buildRunClosure) {
        String urlPath = urlPath(version, platform);
        return WiremockFixture.withWireMock(urlPath, filebytes(urlPath)) { server ->
            File initFile = new File(gradleRunner.getProjectDir(), INIT_SCRIPT)
            initFile.text = """allprojects { p ->
                p.repositories.all { repo ->
                    repo.allowInsecureProtocol = true
                    repo.setUrl('${server.baseUrl()}')
                }
            }"""
            List<String> givenArguments = gradleRunner.getArguments()
            GradleRunner effectiveRunner = gradleRunner.withArguments(givenArguments + ['-I', initFile.getAbsolutePath()])
            buildRunClosure.delegate = effectiveRunner
            return buildRunClosure.call(effectiveRunner)
        }
    }

    private static String urlPath(String version,ElasticsearchDistribution.Platform platform) {
        String fileType = ((platform == ElasticsearchDistribution.Platform.LINUX ||
                platform == ElasticsearchDistribution.Platform.DARWIN)) ? "tar.gz" : "zip"
        String arch = Architecture.current() == Architecture.AARCH64 ? "aarch64" : "x86_64"
        "/downloads/elasticsearch/elasticsearch-${version}-${platform}-${arch}.$fileType"
    }

    private static byte[] filebytes(String urlPath) throws IOException {
        String suffix = urlPath.endsWith("zip") ? "zip" : "tar.gz";
        return DistributionDownloadFixture.getResourceAsStream("/org/elasticsearch/gradle/fake_elasticsearch." + suffix).getBytes()
    }
}
