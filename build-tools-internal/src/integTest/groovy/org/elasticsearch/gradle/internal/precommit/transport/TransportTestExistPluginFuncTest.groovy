/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit.transport

import spock.lang.Shared

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.elasticsearch.gradle.fixtures.AbstractGradleInternalPluginFuncTest
import org.elasticsearch.gradle.fixtures.LocalRepositoryFixture
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin
import org.elasticsearch.gradle.internal.precommit.CheckstylePrecommitPlugin
import org.gradle.api.Plugin
import org.gradle.testkit.runner.TaskOutcome
import org.junit.ClassRule

class TransportTestExistPluginFuncTest extends AbstractGradleInternalPluginFuncTest {
    Class<? extends TransportTestExistPrecommitPlugin> pluginClassUnderTest = TransportTestExistPrecommitPlugin.class

    @Shared
    @ClassRule
    public LocalRepositoryFixture repository = new LocalRepositoryFixture()

    def setup() {
        configurationCacheCompatible = false

        buildFile << """
            apply plugin: 'java'

            repositories {
                mavenCentral()
            }

            """
        repository.configureBuild(buildFile)
    }

    def "can scan transport classes"() {
        given:
        file("src/main/java/org/acme/NonWriteable.java") << """
            package org.acme;

            public class NonWriteable{

            }
        """

        when:
        def result2 = gradleRunner(":transportTestExistCheck").build()

        then:
        result2.task(":transportTestExistCheck").outcome == TaskOutcome.SUCCESS
    }
//        file("src/main/java/org/acme/A.java") << """
//            package org.acme;
//
//            import org.elasticsearch.common.io.stream.Writeable;
//            import org.elasticsearch.common.io.stream.StreamInput;
//            import org.elasticsearch.common.io.stream.StreamOutput;
//
//            public class A extends Writeable {
//             public A(StreamInput input){}
//             public void writeTo(StreamOutput output){}
//            }
//        """
//
//        file("src/test/java/org/acme/ATests.java") << """
//            package org.acme;
//            import org.elasticsearch.test.AbstractWireTestCase;
//            import java.io.IOException;
//
//            public class ATests extends AbstractWireTestCase<A> {
//                @Override
//                protected A createTestInstance() {
//                    return null;
//                }
//
//                @Override
//                protected A mutateInstance(A instance) throws IOException {
//                    return null;
//                }
//
//                @Override
//                protected A copyInstance(A instance, TransportVersion version) throws IOException {
//                    return null;
//                }
//            }
//        """
}
