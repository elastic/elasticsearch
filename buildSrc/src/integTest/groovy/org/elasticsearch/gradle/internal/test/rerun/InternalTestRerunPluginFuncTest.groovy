/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rerun

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest

class InternalTestRerunPluginFuncTest extends AbstractGradleFuncTest {

    def "all tests are rerun when test jvm has crashed"() {
        when:
        buildFile.text = """
        plugins {
          id 'java'
          id 'elasticsearch.internal-test-rerun'
        }

        repositories {
            mavenCentral()
        }
        
        dependencies {
            testImplementation 'junit:junit:4.13.1'
        }
        
        tasks.named("test").configure {
            testLogging {
                // set options for log level LIFECYCLE
                events "started", "passed", "standard_out", "failed"
                exceptionFormat "short"
            }
        }
        
        """
        file("src/test/java/org/acme/JdkKillingTest.java") << """
            import org.junit.Test;
            import static org.junit.Assert.*;
            import java.nio.*;
            import java.nio.file.*;
            import java.io.IOException;
            
            public class JdkKillingTest {
                Path executionLogPath = Paths.get("test-executions" + getClass().getSimpleName() +".log");
                
                @Test 
                public void someTest() {
                    logExecution();
                    if(countExecutions() < 2) {
                        thisKillsTheTestJvm();
                    }
                }
          
                static void thisKillsTheTestJvm() {
                    System.exit(1);
                }
                
                int countExecutions() {
                    try {
                        int total =  Files.readAllLines(executionLogPath).size();
                        System.out.println("JdkKillingTest total executions: " + total);
                        return total;
                    }
                    catch(IOException e) {
                        return 0;
                    }
                }
               
                void logExecution() {
                    String content = "Test executed " + System.currentTimeMillis() + "\\n";
                    try {
                        Files.write(executionLogPath, content.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    } catch (IOException e) {
                        // exception handling
                    }
                    System.out.println("test execution logged at " + executionLogPath.toAbsolutePath().toString());
                }
            }
        """
        then:
        def result = gradleRunner("test").build()
        result.output.contains("JdkKillingTest total executions: 2")
    }
}
