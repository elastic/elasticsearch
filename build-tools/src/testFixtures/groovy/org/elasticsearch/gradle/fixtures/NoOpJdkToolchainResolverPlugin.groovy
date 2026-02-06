/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.fixtures

import org.gradle.api.Plugin
import org.gradle.api.initialization.Settings
import org.gradle.api.services.BuildServiceParameters
import org.gradle.jvm.toolchain.JavaToolchainDownload
import org.gradle.jvm.toolchain.JavaToolchainRequest
import org.gradle.jvm.toolchain.JavaToolchainResolver
import org.gradle.jvm.toolchain.JavaToolchainResolverRegistry

import javax.inject.Inject

abstract class NoOpJdkToolchainResolverPlugin implements Plugin<Settings> {
    @Inject
    protected abstract JavaToolchainResolverRegistry getToolchainResolverRegistry();

    void apply(Settings settings) {
        settings.getPlugins().apply("jvm-toolchain-management");
        JavaToolchainResolverRegistry registry = getToolchainResolverRegistry();
        registry.register(LoggingToolchainResolverFixture.class);
    }

    static class LoggingToolchainResolverFixture implements JavaToolchainResolver {

        @Override
        Optional<JavaToolchainDownload> resolve(JavaToolchainRequest request) {
            println "request.javaToolchainSpec.languageVersion = $request.javaToolchainSpec.languageVersion"

            return Optional.of(
                new JavaToolchainDownload() {
                    @Override
                    URI getUri() {
                        System.getProperty("toolchain.uri")?.with { return URI.create(it) }
                        def uriProperty = System.getProperty("toolchain.uri");
                        return URI.create(uriProperty)
                    }
                }
            )
        }

        @Override
        BuildServiceParameters.None getParameters() {
            return null
        }
    }
}
