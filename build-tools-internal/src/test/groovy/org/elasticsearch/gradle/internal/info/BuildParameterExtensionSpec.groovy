/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.info

import spock.lang.Ignore
import spock.lang.Specification

import org.gradle.api.JavaVersion
import org.gradle.api.Project
import org.gradle.api.provider.Provider
import org.gradle.api.provider.ProviderFactory
import org.gradle.testfixtures.ProjectBuilder
import org.junit.Assert

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import static org.junit.Assert.fail

class BuildParameterExtensionSpec extends Specification {

    ProjectBuilder projectBuilder = new ProjectBuilder()

    @Ignore
    def "#getterName is cached anc concurrently accessible"() {
        given:
        def project = projectBuilder.build()
        def providers = project.getProviders();
        def buildParams = extension(project, providers)
        int numberOfThreads = 10;
        when:
        var service = Executors.newFixedThreadPool(numberOfThreads)
        var latch = new CountDownLatch(numberOfThreads)
        def testedProvider = buildParams."$getterName"()
        def futures = (1..numberOfThreads).collect {
            service.submit(
                () -> {
                    try {
                        testedProvider.get()
                    } catch (AssertionError e) {
                        latch.countDown()
                        Assert.fail("Accessing cached provider more than once")
                    }
                    latch.countDown()
                }
            )
        }
        latch.await(10, TimeUnit.SECONDS)

        then:
        futures.collect { it.state() }.any() { it == Future.State.FAILED } == false

        where:
        getterName << [
            "getRuntimeJavaHome",
            "getJavaToolChainSpec",
            "getRuntimeJavaDetails",
            "getRuntimeJavaVersion",
            "getBwcVersionsProvider"
        ]
    }

    private BuildParameterExtension extension(Project project, ProviderFactory providers) {
        return project.getExtensions().create(
            BuildParameterExtension.class, "buildParameters", DefaultBuildParameterExtension.class,
            providers,
            providerMock(),
            providerMock(),
            providerMock(),
            true,
            providerMock(),
            [
                Mock(JavaHome),
                Mock(JavaHome),
            ],
            JavaVersion.VERSION_11,
            JavaVersion.VERSION_11,
            JavaVersion.VERSION_11,
            providerMock(),
            providerMock(),
            "testSeed",
            false,
            5,
            true,
            // cannot use Mock here because of the way the provider is used by gradle internal property api
            providerMock()
        )
    }

    private Provider providerMock() {
        Provider provider = Mock(Provider)
        AtomicInteger counter = new AtomicInteger(0)
        provider.getOrNull() >> {
            println "accessing provider"
            return counter.get() == 1 ? fail("Accessing cached provider more than once") : counter.incrementAndGet()
        }
        provider.get() >> {
            fail("Accessing cached provider directly")
        }
        return provider

    }
}
