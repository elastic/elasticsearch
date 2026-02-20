/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.fixtures

import spock.lang.Specification
import spock.lang.TempDir
import com.github.tomakehurst.wiremock.WireMockServer

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.elasticsearch.gradle.internal.test.BuildConfigurationAwareGradleRunner
import org.elasticsearch.gradle.internal.test.InternalAwareGradleRunner
import org.elasticsearch.gradle.internal.test.NormalizeOutputGradleRunner
import org.elasticsearch.gradle.internal.test.TestResultExtension
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner
import org.junit.After
import org.junit.Rule
import org.junit.rules.TemporaryFolder

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipFile

import static com.github.tomakehurst.wiremock.client.WireMock.*
import static org.elasticsearch.gradle.internal.test.TestUtils.normalizeString

abstract class AbstractGradleFuncTest extends Specification {

    @Rule
    TemporaryFolder testProjectDir = new TemporaryFolder()

    @TempDir
    File gradleUserHome

    File settingsFile
    File buildFile
    File propertiesFile
    File projectDir
    File versionPropertiesFile

    protected boolean configurationCacheCompatible = true
    protected boolean buildApiRestrictionsDisabled = false

    def setup() {
        projectDir = testProjectDir.root
        settingsFile = testProjectDir.newFile('settings.gradle')
        settingsFile << "rootProject.name = 'hello-world'\n"
        buildFile = testProjectDir.newFile('build.gradle')
        propertiesFile = testProjectDir.newFile('gradle.properties')
        File buildToolsDir = testProjectDir.newFolder("build-tools-internal")
        versionPropertiesFile = new File(buildToolsDir, 'version.properties')
        versionPropertiesFile.text = """
            elasticsearch     = 9.1.0
            lucene            = 10.2.2

            bundled_jdk_vendor = openjdk
            bundled_jdk = 24+36@1f9ff9062db4449d8ca828c504ffae90
            minimumJdkVersion = 21
            minimumRuntimeJava = 21
            minimumCompilerJava = 21
        """
        propertiesFile <<
            "org.gradle.java.installations.fromEnv=JAVA_HOME,RUNTIME_JAVA_HOME,JAVA15_HOME,JAVA14_HOME,JAVA13_HOME,JAVA12_HOME,JAVA11_HOME,JAVA8_HOME"

        def nativeLibsProject = subProject(":libs:native:native-libraries")
        nativeLibsProject << """
            plugins {
                id 'base'
            }
        """
        def mutedTestsFile = testProjectDir.newFile("muted-tests.yml")
        mutedTestsFile << """
            tests: []
        """
    }

    def cleanup() {
        if (featureFailed()) {
            FileUtils.copyDirectory(testProjectDir.root, new File("build/test-debug/" + testProjectDir.root.name))
        }
    }

    File subProject(String subProjectPath) {
        def subProjectBuild = file(subProjectPath.replace(":", "/") + "/build.gradle")
        if (subProjectBuild.exists() == false) {
            settingsFile << "include \"${subProjectPath}\"\n"
        }
        subProjectBuild.parentFile.mkdirs()
        return subProjectBuild
    }

    File subProject(String subProjectPath, Closure configAction) {
        def subProjectBuild = subProject(subProjectPath)
        configAction.setDelegate(new ProjectConfigurer(subProjectBuild.parentFile))
        configAction.setResolveStrategy(Closure.DELEGATE_ONLY)
        configAction.call()
        subProjectBuild
    }


    GradleRunner gradleRunner(Object... arguments) {
        return gradleRunner(testProjectDir.root, arguments)
    }

    GradleRunner gradleRunner(File projectDir, Object... arguments) {
        return new NormalizeOutputGradleRunner(
            new BuildConfigurationAwareGradleRunner(
                    new InternalAwareGradleRunner(
                        GradleRunner.create()
                                .withDebug(ManagementFactory.getRuntimeMXBean().getInputArguments()
                                        .toString().indexOf("-agentlib:jdwp") > 0
                                )
                                .withProjectDir(projectDir)
                                .withPluginClasspath()
                                .forwardOutput()
            ), configurationCacheCompatible,
                buildApiRestrictionsDisabled)
        ).withArguments(arguments.collect { it.toString() } + "--full-stacktrace")
    }

    def assertOutputContains(String givenOutput, String expected) {
        assert normalized(givenOutput).contains(normalized(expected))
        true
    }

    def assertNoDeprecationWarning(BuildResult result) {
        assertOutputMissing(result.getOutput(), "Deprecated Gradle features were used in this build");
    }

    def assertOutputMissing(String givenOutput, String expected) {
        assert normalized(givenOutput).contains(normalized(expected)) == false
        true
    }

    String normalized(String input) {
        return normalizeString(input, testProjectDir.root)
    }

    File file(String path) {
        return file(testProjectDir.root, path)
    }

    File file(File parent, String path) {
        File newFile = new File(parent, path)
        newFile.getParentFile().mkdirs()
        newFile
    }

    File someJar(String fileName = 'some.jar') {
        File jarFolder = new File(testProjectDir.root, "jars");
        jarFolder.mkdirs()
        File jarFile = new File(jarFolder, fileName)
        JarEntry entry = new JarEntry("foo.txt");

        jarFile.withOutputStream {
            JarOutputStream target = new JarOutputStream(it)
            target.putNextEntry(entry);
            target.closeEntry();
            target.close();
        }

        return jarFile;
    }

    File internalBuild(
        List<String> extraPlugins = [],
        String maintenance = "7.16.10",
        String major4 = "8.1.3",
        String major3 = "8.2.1",
        String major2 = "8.3.0",
        String major1 = "8.4.0",
        String current = "9.0.0"
    ) {
        buildFile << """plugins {
          id 'elasticsearch.global-build-info'
          ${extraPlugins.collect { p -> "id '$p'" }.join('\n')}
        }
        import org.elasticsearch.gradle.Architecture

        import org.elasticsearch.gradle.internal.BwcVersions
        import org.elasticsearch.gradle.internal.info.DevelopmentBranch
        import org.elasticsearch.gradle.Version

        Version currentVersion = Version.fromString("${current}")
        def versionList = [
          Version.fromString("$maintenance"),
          Version.fromString("$major4"),
          Version.fromString("$major3"),
          Version.fromString("$major2"),
          Version.fromString("$major1"),
          currentVersion
        ]

        BwcVersions versions = new BwcVersions(currentVersion, versionList, [
          new DevelopmentBranch('main', Version.fromString("$current")),
          new DevelopmentBranch('8.x', Version.fromString("$major1")),
          new DevelopmentBranch('8.3', Version.fromString("$major2")),
          new DevelopmentBranch('8.2', Version.fromString("$major3")),
          new DevelopmentBranch('8.1', Version.fromString("$major4")),
          new DevelopmentBranch('7.16', Version.fromString("$maintenance")),
        ])
        buildParams.setBwcVersions(project.provider { versions } )
        """
    }

    String execute(String command, File workingDir = testProjectDir.root, boolean ignoreFailure = false) {
        def proc = command.execute(Collections.emptyList(), workingDir)
        proc.waitFor()
        if (proc.exitValue() && ignoreFailure == false) {
            String msg = """Error running command ${command}:
                Sysout: ${proc.inputStream.text}
                Syserr: ${proc.errorStream.text}
            """
            throw new RuntimeException(msg)
        }
        return proc.inputStream.text
    }

    File dir(String path) {
        def dir = file(projectDir, path)
        dir.mkdirs()
        dir
    }

    void withVersionCatalogue() {
        file('build.versions.toml') << '''\
[libraries]
checkstyle = "com.puppycrawl.tools:checkstyle:10.3"
'''
        settingsFile << '''
            dependencyResolutionManagement {
              versionCatalogs {
                buildLibs {
                  from(files("build.versions.toml"))
                }
              }
            }
            '''
    }

    boolean featureFailed() {
        specificationContext.currentSpec.listeners
            .findAll { it instanceof TestResultExtension.ErrorListener }
            .any {
                (it as TestResultExtension.ErrorListener).errorInfo != null
            }
    }

    ZipAssertion zip(String relativePath) {
        File archiveFile = file(relativePath);
        try (ZipFile zipFile = new ZipFile(archiveFile)) {
            Map<String, ZipAssertionFile> files = zipFile.entries().collectEntries { ZipEntry entry ->
                [(entry.name): new ZipAssertionFile(archiveFile, entry)]
            }
            return new ZipAssertion(files);
        }
    }

    static class ZipAssertion {
        private Map<String, ZipAssertionFile> files = new HashMap<>()

        ZipAssertion(Map<String, ZipAssertionFile> files) {
            this.files = files;
        }

        ZipAssertionFile file(String path) {
            return this.files.get(path)
        }

        Collection<ZipAssertionFile> files() {
            return files.values()
        }
    }

    static class ZipAssertionFile {

        private ZipEntry entry;
        private File zipFile;

        ZipAssertionFile(File zipFile, ZipEntry entry) {
            this.entry = entry
            this.zipFile = zipFile
        }

        boolean exists() {
            entry == null
        }

        String getName() {
            return entry.name
        }

        boolean isDirectory() {
            return entry.isDirectory()
        }

        String read() {
            try (ZipFile zipFile1 = new ZipFile(zipFile)) {
                def inputStream = zipFile1.getInputStream(entry)
                return IOUtils.toString(inputStream, StandardCharsets.UTF_8.name())
            } catch (IOException e) {
                throw new RuntimeException("Failed to read entry ${entry.name} from zip file ${zipFile.name}", e)
            }
        }
    }

    static class ProjectConfigurer {
        private File projectDir

        ProjectConfigurer(File projectDir) {
            this.projectDir = projectDir
        }

        File classFile(String fullQualifiedName) {
            File sourceRoot = new File(projectDir, 'src/main/java');
            File file = new File(sourceRoot, fullQualifiedName.replace('.', '/') + '.java')
            file.getParentFile().mkdirs()
            file
        }

        File getBuildFile() {
            return new File(projectDir, 'build.gradle')
        };

        File file(String path) {
            def file = new File(projectDir, path)
            file.parentFile.mkdirs()
            file
        }

        File createTest(String clazzName, String content = testMethodContent(false, false, 1)) {
            def file = new File(projectDir, "src/test/java/org/acme/${clazzName}.java")
            file.parentFile.mkdirs()
            file << """
            package org.acme;

            import org.junit.Test;
            import org.junit.Before;
            import org.junit.After;
            import org.junit.Assert;
            import java.nio.*;
            import java.nio.file.*;
            import java.io.IOException;

            public class $clazzName {

                @Before
                public void beforeTest() {
                }

                @After
                public void afterTest() {
                }

                @Test
                public void someTest1() {
                    ${content}
                }

                @Test
                public void someTest2() {
                    ${content}
                }
            }
        """
        }

        String testMethodContent(boolean withSystemExit, boolean fail, int timesFailing = 1) {
            return """
            System.out.println(getClass().getSimpleName() + " executing");

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

    }
}
