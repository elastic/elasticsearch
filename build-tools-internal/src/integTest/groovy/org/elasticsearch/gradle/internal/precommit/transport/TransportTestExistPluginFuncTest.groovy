/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit.transport


import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

class TransportTestExistPluginFuncTest extends AbstractGradleFuncTest {

    def setup() {
        configurationCacheCompatible = false

        buildFile << """
            apply plugin:'java'

            group = 'org.acme'
            description = "some example project"
            version = '1.2.3'
            repositories {
                mavenCentral()
            }

            """
    }

    def "can scan transport classes"() {
        given:
        file("src/main/java/org/acme/A.java") << """
            package org.acme;

            import org.elasticsearch.common.io.stream.Writeable;
            import org.elasticsearch.common.io.stream.StreamInput;
            import org.elasticsearch.common.io.stream.StreamOutput;

            public class A extends Writeable {
             public A(StreamInput input){}
             public void writeTo(StreamOutput output){}
            }
        """

//        file("src/main/java/org/acme/Missing.java") << """
//            package org.acme;
//
//            import org.elasticsearch.common.io.stream.Writeable;
//            import org.elasticsearch.common.io.stream.StreamInput;
//            import org.elasticsearch.common.io.stream.StreamOutput;
//
//            public class Missing extends Writeable {
//             public Missing(StreamInput input){}
//             public void writeTo(StreamOutput output){}
//            }
//        """

        file("src/test/java/org/acme/ATests.java") << """
            package org.acme;
            import org.elasticsearch.test.AbstractWireTestCase;
            import java.io.IOException;

            public class ATests extends AbstractWireTestCase<A> {
                @Override
                protected A createTestInstance() {
                    return null;
                }

                @Override
                protected A mutateInstance(A instance) throws IOException {
                    return null;
                }

                @Override
                protected A copyInstance(A instance, TransportVersion version) throws IOException {
                    return null;
                }
            }
        """

        when:
        def result = gradleRunner(":precommit").build()

        then:
        result.task(":precommit").outcome == TaskOutcome.FAILURE



    }

}
