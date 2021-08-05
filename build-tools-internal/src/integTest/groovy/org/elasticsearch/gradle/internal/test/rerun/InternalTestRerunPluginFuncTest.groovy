/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rerun

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import spock.lang.Ignore

@Ignore
class InternalTestRerunPluginFuncTest extends AbstractGradleFuncTest {

    def "does not rerun on failed tests"() {
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
            maxParallelForks = 4
            testLogging {
                events "standard_out", "failed"
                exceptionFormat "short"
            }
        }
        
        """
        createTest("SimpleTest")
        createTest("SimpleTest2")
        createTest("SimpleTest3")
        createTest("SimpleTest4")
        createTest("SimpleTest5")
        createFailedTest("SimpleTest6")
        createFailedTest("SimpleTest7")
        createFailedTest("SimpleTest8")
        createTest("SomeOtherTest")
        createTest("SomeOtherTest1")
        createTest("SomeOtherTest2")
        createTest("SomeOtherTest3")
        createTest("SomeOtherTest4")
        createTest("SomeOtherTest5")
        then:
        def result = gradleRunner("test").buildAndFail()
        result.output.contains("total executions: 2") == false
        and: "no jvm system exit tracing provided"
        normalized(result.output).contains("""Test jvm exited unexpectedly.
Test jvm system exit trace:""") == false
    }

    def "all tests are rerun when test jvm has crashed"() {
        when:
        settingsFile.text = """
        plugins {
            id "com.gradle.enterprise" version "3.6.1"
        }
        gradleEnterprise {
            server = 'https://gradle-enterprise.elastic.co/'
        }
        """ + settingsFile.text

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
            maxParallelForks = 4
            testLogging {
                // set options for log level LIFECYCLE
                events "started", "passed", "standard_out", "failed"
                exceptionFormat "short"
            }
        }
        
        """
        createTest("AnotherTest")
        createTest("AnotherTest2")
        createTest("AnotherTest3")
        createTest("AnotherTest4")
        createTest("AnotherTest5")
        createSystemExitTest("AnotherTest6")
        createTest("AnotherTest7")
        createTest("AnotherTest8")
        createTest("AnotherTest10")
        createTest("SimpleTest")
        createTest("SimpleTest2")
        createTest("SimpleTest3")
        createTest("SimpleTest4")
        createTest("SimpleTest5")
        createTest("SimpleTest6")
        createTest("SimpleTest7")
        createTest("SimpleTest8")
        createTest("SomeOtherTest")
        then:
        def result = gradleRunner("test").build()
        result.output.contains("AnotherTest6 total executions: 2")
        // triggered only in the second overall run
        and: 'Tracing is provided'
        normalized(result.output).contains("""================
Test jvm exited unexpectedly.
Test jvm system exit trace (run: 1)
Gradle Test Executor 1 > AnotherTest6 > someTest
================""")
    }

    def "rerun build fails due to any test failure"() {
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
            maxParallelForks = 5
            testLogging {
                events "started", "passed", "standard_out", "failed"
                exceptionFormat "short"
            }
        }
        
        """
        createSystemExitTest("AnotherTest6")
        createFailedTest("SimpleTest1")
        createFailedTest("SimpleTest2")
        createFailedTest("SimpleTest3")
        createFailedTest("SimpleTest4")
        createFailedTest("SimpleTest5")
        createFailedTest("SimpleTest6")
        createFailedTest("SimpleTest7")
        createFailedTest("SimpleTest8")
        createFailedTest("SimpleTest9")
        then:
        def result = gradleRunner("test").buildAndFail()
        result.output.contains("AnotherTest6 total executions: 2")
        result.output.contains("> There were failing tests. See the report at:")
    }

    def "reruns tests till max rerun count is reached"() {
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
            rerun {
                maxReruns = 4
            }
            testLogging {
                // set options for log level LIFECYCLE
                events "standard_out", "failed"
                exceptionFormat "short"
            }
        }
        """
        createSystemExitTest("JdkKillingTest", 5)
        then:
        def result = gradleRunner("test").buildAndFail()
        result.output.contains("JdkKillingTest total executions: 5")
        result.output.contains("Max retries(4) hit")
        and: 'Tracing is provided'
        normalized(result.output).contains("Test jvm system exit trace (run: 1)")
        normalized(result.output).contains("Test jvm system exit trace (run: 2)")
        normalized(result.output).contains("Test jvm system exit trace (run: 3)")
        normalized(result.output).contains("Test jvm system exit trace (run: 4)")
        normalized(result.output).contains("Test jvm system exit trace (run: 5)")
    }

    private String testMethodContent(boolean withSystemExit, boolean fail, int timesFailing = 1) {
        return """
            int count = countExecutions();
            System.out.println(getClass().getSimpleName() + " total executions: " + count);

            ${withSystemExit ? """
                    if(count <= ${timesFailing}) {
                        System.exit(1);
                    }
                    """ : ''
            }

            ${fail ? """
                    if(count <= ${timesFailing}) {
                        try {
                            Thread.sleep(2000);
                        } catch(Exception e) {}
                        Assert.fail();
                    }
                    """ : ''
            }
        """
    }

    private File createSystemExitTest(String clazzName, timesFailing = 1) {
        createTest(clazzName, testMethodContent(true, false, timesFailing))
    }
    private File createFailedTest(String clazzName) {
        createTest(clazzName, testMethodContent(false, true, 1))
    }

    private File createTest(String clazzName, String content = testMethodContent(false, false, 1)) {
        file("src/test/java/org/acme/${clazzName}.java") << """
            import org.junit.Test;
            import org.junit.Before;
            import org.junit.After;
            import org.junit.Assert;
            import java.nio.*;
            import java.nio.file.*;
            import java.io.IOException;
            
            public class $clazzName {
                Path executionLogPath = Paths.get("test-executions" + getClass().getSimpleName() +".log");
                
                @Before 
                public void beforeTest() {
                    logExecution();
                }
                
                @After 
                public void afterTest() {
                }
                
                @Test 
                public void someTest() {
                    ${content}
                }
                
                int countExecutions() {
                    try {
                        return Files.readAllLines(executionLogPath).size();
                    }
                    catch(IOException e) {
                        return 0;
                    }
                }
               
                void logExecution() {
                    try {
                        Files.write(executionLogPath, "Test executed\\n".getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    } catch (IOException e) {
                        // exception handling
                    }
                }
            }
        """
    }
}
