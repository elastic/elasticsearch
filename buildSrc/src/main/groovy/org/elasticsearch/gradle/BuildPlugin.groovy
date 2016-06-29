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
package org.elasticsearch.gradle

import nebula.plugin.extraconfigurations.ProvidedBasePlugin
import nebula.plugin.publishing.maven.MavenBasePublishPlugin
import org.elasticsearch.gradle.precommit.PrecommitTasks
import org.gradle.api.GradleException
import org.gradle.api.JavaVersion
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.XmlProvider
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.ModuleDependency
import org.gradle.api.artifacts.ModuleVersionIdentifier
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.artifacts.ResolvedArtifact
import org.gradle.api.artifacts.dsl.RepositoryHandler
import org.gradle.api.artifacts.maven.MavenPom
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.maven.tasks.GenerateMavenPom
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.internal.jvm.Jvm
import org.gradle.process.ExecResult
import org.gradle.util.GradleVersion

import java.time.ZoneOffset
import java.time.ZonedDateTime

/**
 * Encapsulates build configuration for elasticsearch projects.
 */
class BuildPlugin implements Plugin<Project> {

    static final JavaVersion minimumJava = JavaVersion.VERSION_1_8

    @Override
    void apply(Project project) {
        project.pluginManager.apply('java')
        project.pluginManager.apply('carrotsearch.randomized-testing')
        // these plugins add lots of info to our jars
        configureJars(project) // jar config must be added before info broker
        project.pluginManager.apply('nebula.info-broker')
        project.pluginManager.apply('nebula.info-basic')
        project.pluginManager.apply('nebula.info-java')
        project.pluginManager.apply('nebula.info-scm')
        project.pluginManager.apply('nebula.info-jar')
        project.pluginManager.apply('com.bmuschko.nexus')
        project.pluginManager.apply(ProvidedBasePlugin)

        globalBuildInfo(project)
        configureRepositories(project)
        configureConfigurations(project)
        project.ext.versions = VersionProperties.versions
        configureCompile(project)
        configurePomGeneration(project)

        configureTest(project)
        configurePrecommit(project)
    }

    /** Performs checks on the build environment and prints information about the build environment. */
    static void globalBuildInfo(Project project) {
        if (project.rootProject.ext.has('buildChecksDone') == false) {
            String javaHome = findJavaHome()
            File gradleJavaHome = Jvm.current().javaHome
            String javaVendor = System.getProperty('java.vendor')
            String javaVersion = System.getProperty('java.version')
            String gradleJavaVersionDetails = "${javaVendor} ${javaVersion}" +
                " [${System.getProperty('java.vm.name')} ${System.getProperty('java.vm.version')}]"

            String javaVersionDetails = gradleJavaVersionDetails
            JavaVersion javaVersionEnum = JavaVersion.current()
            if (new File(javaHome).canonicalPath != gradleJavaHome.canonicalPath) {
                javaVersionDetails = findJavaVersionDetails(project, javaHome)
                javaVersionEnum = JavaVersion.toVersion(findJavaSpecificationVersion(project, javaHome))
                javaVendor = findJavaVendor(project, javaHome)
                javaVersion = findJavaVersion(project, javaHome)
            }

            // Build debugging info
            println '======================================='
            println 'Elasticsearch Build Hamster says Hello!'
            println '======================================='
            println "  Gradle Version        : ${project.gradle.gradleVersion}"
            println "  OS Info               : ${System.getProperty('os.name')} ${System.getProperty('os.version')} (${System.getProperty('os.arch')})"
            if (gradleJavaVersionDetails != javaVersionDetails) {
                println "  JDK Version (gradle)  : ${gradleJavaVersionDetails}"
                println "  JAVA_HOME (gradle)    : ${gradleJavaHome}"
                println "  JDK Version (compile) : ${javaVersionDetails}"
                println "  JAVA_HOME (compile)   : ${javaHome}"
            } else {
                println "  JDK Version           : ${gradleJavaVersionDetails}"
                println "  JAVA_HOME             : ${gradleJavaHome}"
            }

            // enforce gradle version
            GradleVersion minGradle = GradleVersion.version('2.13')
            if (GradleVersion.current() < minGradle) {
                throw new GradleException("${minGradle} or above is required to build elasticsearch")
            }

            // enforce Java version
            if (javaVersionEnum < minimumJava) {
                throw new GradleException("Java ${minimumJava} or above is required to build Elasticsearch")
            }

            // this block of code detecting buggy JDK 8 compiler versions can be removed when minimum Java version is incremented
            assert minimumJava == JavaVersion.VERSION_1_8 : "Remove JDK compiler bug detection only applicable to JDK 8"
            if (javaVersionEnum == JavaVersion.VERSION_1_8) {
                if (Objects.equals("Oracle Corporation", javaVendor)) {
                    def matcher = javaVersion =~ /1\.8\.0(?:_(\d+))?/
                    if (matcher.matches()) {
                        int update;
                        if (matcher.group(1) == null) {
                            update = 0
                        } else {
                            update = matcher.group(1).toInteger()
                        }
                        if (update < 40) {
                            throw new GradleException("JDK ${javaVendor} ${javaVersion} has compiler bug JDK-8052388, update your JDK to at least 8u40")
                        }
                    }
                }
            }

            project.rootProject.ext.javaHome = javaHome
            project.rootProject.ext.javaVersion = javaVersionEnum
            project.rootProject.ext.buildChecksDone = true
        }
        project.targetCompatibility = minimumJava
        project.sourceCompatibility = minimumJava
        // set java home for each project, so they dont have to find it in the root project
        project.ext.javaHome = project.rootProject.ext.javaHome
        project.ext.javaVersion = project.rootProject.ext.javaVersion
    }

    /** Finds and enforces JAVA_HOME is set */
    private static String findJavaHome() {
        String javaHome = System.getenv('JAVA_HOME')
        if (javaHome == null) {
            if (System.getProperty("idea.active") != null) {
                // intellij doesn't set JAVA_HOME, so we use the jdk gradle was run with
                javaHome = Jvm.current().javaHome
            } else {
                throw new GradleException('JAVA_HOME must be set to build Elasticsearch')
            }
        }
        return javaHome
    }

    /** Finds printable java version of the given JAVA_HOME */
    private static String findJavaVersionDetails(Project project, String javaHome) {
        String versionInfoScript = 'print(' +
            'java.lang.System.getProperty("java.vendor") + " " + java.lang.System.getProperty("java.version") + ' +
            '" [" + java.lang.System.getProperty("java.vm.name") + " " + java.lang.System.getProperty("java.vm.version") + "]");'
        return runJavascript(project, javaHome, versionInfoScript).trim()
    }

    /** Finds the parsable java specification version */
    private static String findJavaSpecificationVersion(Project project, String javaHome) {
        String versionScript = 'print(java.lang.System.getProperty("java.specification.version"));'
        return runJavascript(project, javaHome, versionScript)
    }

    private static String findJavaVendor(Project project, String javaHome) {
        String vendorScript = 'print(java.lang.System.getProperty("java.vendor"));'
        return runJavascript(project, javaHome, vendorScript)
    }

    /** Finds the parsable java specification version */
    private static String findJavaVersion(Project project, String javaHome) {
        String versionScript = 'print(java.lang.System.getProperty("java.version"));'
        return runJavascript(project, javaHome, versionScript)
    }

    /** Runs the given javascript using jjs from the jdk, and returns the output */
    private static String runJavascript(Project project, String javaHome, String script) {
        File tmpScript = File.createTempFile('es-gradle-tmp', '.js')
        tmpScript.setText(script, 'UTF-8')
        ByteArrayOutputStream output = new ByteArrayOutputStream()
        ExecResult result = project.exec {
            executable = new File(javaHome, 'bin/jjs')
            args tmpScript.toString()
            standardOutput = output
            errorOutput = new ByteArrayOutputStream()
            ignoreExitValue = true // we do not fail so we can first cleanup the tmp file
        }
        java.nio.file.Files.delete(tmpScript.toPath())
        result.assertNormalExitValue()
        return output.toString('UTF-8').trim()
    }

    /** Return the configuration name used for finding transitive deps of the given dependency. */
    private static String transitiveDepConfigName(String groupId, String artifactId, String version) {
        return "_transitive_${groupId}:${artifactId}:${version}"
    }

    /**
     * Makes dependencies non-transitive.
     *
     * Gradle allows setting all dependencies as non-transitive very easily.
     * Sadly this mechanism does not translate into maven pom generation. In order
     * to effectively make the pom act as if it has no transitive dependencies,
     * we must exclude each transitive dependency of each direct dependency.
     *
     * Determining the transitive deps of a dependency which has been resolved as
     * non-transitive is difficult because the process of resolving removes the
     * transitive deps. To sidestep this issue, we create a configuration per
     * direct dependency version. This specially named and unique configuration
     * will contain all of the transitive dependencies of this particular
     * dependency. We can then use this configuration during pom generation
     * to iterate the transitive dependencies and add excludes.
     */
    static void configureConfigurations(Project project) {
        // we are not shipping these jars, we act like dumb consumers of these things
        if (project.path.startsWith(':test:fixtures') || project.path == ':build-tools') {
            return
        }
        // fail on any conflicting dependency versions
        project.configurations.all({ Configuration configuration ->
            if (configuration.name.startsWith('_transitive_')) {
                // don't force transitive configurations to not conflict with themselves, since
                // we just have them to find *what* transitive deps exist
                return
            }
            if (configuration.name.endsWith('Fixture')) {
                // just a self contained test-fixture configuration, likely transitive and hellacious
                return
            }
            configuration.resolutionStrategy.failOnVersionConflict()
        })

        // force all dependencies added directly to compile/testCompile to be non-transitive, except for ES itself
        Closure disableTransitiveDeps = { ModuleDependency dep ->
            if (!(dep instanceof ProjectDependency) && dep.group.startsWith('org.elasticsearch') == false) {
                dep.transitive = false

                // also create a configuration just for this dependency version, so that later
                // we can determine which transitive dependencies it has
                String depConfig = transitiveDepConfigName(dep.group, dep.name, dep.version)
                if (project.configurations.findByName(depConfig) == null) {
                    project.configurations.create(depConfig)
                    project.dependencies.add(depConfig, "${dep.group}:${dep.name}:${dep.version}")
                }
            }
        }

        project.configurations.compile.dependencies.all(disableTransitiveDeps)
        project.configurations.testCompile.dependencies.all(disableTransitiveDeps)
        project.configurations.provided.dependencies.all(disableTransitiveDeps)

        // add exclusions to the pom directly, for each of the transitive deps of this project's deps
        project.modifyPom { MavenPom pom ->
            pom.withXml(removeTransitiveDependencies(project))
        }
    }

    /** Adds repositores used by ES dependencies */
    static void configureRepositories(Project project) {
        RepositoryHandler repos = project.repositories
        if (System.getProperty("repos.mavenlocal") != null) {
            // with -Drepos.mavenlocal=true we can force checking the local .m2 repo which is
            // useful for development ie. bwc tests where we install stuff in the local repository
            // such that we don't have to pass hardcoded files to gradle
            repos.mavenLocal()
        }
        repos.mavenCentral()
        repos.maven {
            name 'sonatype-snapshots'
            url 'http://oss.sonatype.org/content/repositories/snapshots/'
        }
        String luceneVersion = VersionProperties.lucene
        if (luceneVersion.contains('-snapshot')) {
            // extract the revision number from the version with a regex matcher
            String revision = (luceneVersion =~ /\w+-snapshot-([a-z0-9]+)/)[0][1]
            repos.maven {
                name 'lucene-snapshots'
                url "http://s3.amazonaws.com/download.elasticsearch.org/lucenesnapshots/${revision}"
            }
        }
    }

    /** Returns a closure which can be used with a MavenPom for removing transitive dependencies. */
    private static Closure removeTransitiveDependencies(Project project) {
        // TODO: remove this when enforcing gradle 2.13+, it now properly handles exclusions
        return { XmlProvider xml ->
            // first find if we have dependencies at all, and grab the node
            NodeList depsNodes = xml.asNode().get('dependencies')
            if (depsNodes.isEmpty()) {
                return
            }

            // check each dependency for any transitive deps
            for (Node depNode : depsNodes.get(0).children()) {
                String groupId = depNode.get('groupId').get(0).text()
                String artifactId = depNode.get('artifactId').get(0).text()
                String version = depNode.get('version').get(0).text()

                // collect the transitive deps now that we know what this dependency is
                String depConfig = transitiveDepConfigName(groupId, artifactId, version)
                Configuration configuration = project.configurations.findByName(depConfig)
                if (configuration == null) {
                    continue // we did not make this dep non-transitive
                }
                Set<ResolvedArtifact> artifacts = configuration.resolvedConfiguration.resolvedArtifacts
                if (artifacts.size() <= 1) {
                    // this dep has no transitive deps (or the only artifact is itself)
                    continue
                }

                // we now know we have something to exclude, so add the exclusion elements
                Node exclusions = depNode.appendNode('exclusions')
                for (ResolvedArtifact transitiveArtifact : artifacts) {
                    ModuleVersionIdentifier transitiveDep = transitiveArtifact.moduleVersion.id
                    if (transitiveDep.group == groupId && transitiveDep.name == artifactId) {
                        continue; // don't exclude the dependency itself!
                    }
                    Node exclusion = exclusions.appendNode('exclusion')
                    exclusion.appendNode('groupId', transitiveDep.group)
                    exclusion.appendNode('artifactId', transitiveDep.name)
                }
            }
        }
    }

    /**Configuration generation of maven poms. */
    public static void configurePomGeneration(Project project) {
        project.plugins.withType(MavenBasePublishPlugin.class).whenPluginAdded {
            project.publishing {
                publications {
                    all { MavenPublication publication -> // we only deal with maven
                        // add exclusions to the pom directly, for each of the transitive deps of this project's deps
                        publication.pom.withXml(removeTransitiveDependencies(project))
                    }
                }
            }

            project.tasks.withType(GenerateMavenPom.class) { GenerateMavenPom t ->
                // place the pom next to the jar it is for
                t.destination = new File(project.buildDir, "distributions/${project.archivesBaseName}-${project.version}.pom")
                // build poms with assemble
                project.assemble.dependsOn(t)
            }
        }
    }

    /** Adds compiler settings to the project */
    static void configureCompile(Project project) {
        project.ext.compactProfile = 'compact3'
        project.afterEvaluate {
            // fail on all javac warnings
            project.tasks.withType(JavaCompile) {
                options.fork = true
                options.forkOptions.executable = new File(project.javaHome, 'bin/javac')
                options.forkOptions.memoryMaximumSize = "1g"
                if (project.targetCompatibility >= JavaVersion.VERSION_1_8) {
                    // compile with compact 3 profile by default
                    // NOTE: this is just a compile time check: does not replace testing with a compact3 JRE
                    if (project.compactProfile != 'full') {
                        options.compilerArgs << '-profile' << project.compactProfile
                    }
                }
                /*
                 * -path because gradle will send in paths that don't always exist.
                 * -missing because we have tons of missing @returns and @param.
                 * -serial because we don't use java serialization.
                 */
                // don't even think about passing args with -J-xxx, oracle will ask you to submit a bug report :)
                options.compilerArgs << '-Werror' << '-Xlint:all,-path,-serial,-options,-deprecation' << '-Xdoclint:all' << '-Xdoclint:-missing'
                options.encoding = 'UTF-8'
                //options.incremental = true

                if (project.javaVersion == JavaVersion.VERSION_1_9) {
                    // hack until gradle supports java 9's new "-release" arg
                    assert minimumJava == JavaVersion.VERSION_1_8
                    options.compilerArgs << '-release' << '8'
                    project.sourceCompatibility = null
                    project.targetCompatibility = null
                }
            }
        }
    }

    /** Adds additional manifest info to jars, and adds source and javadoc jars */
    static void configureJars(Project project) {
        project.tasks.withType(Jar) { Jar jarTask ->
            // we put all our distributable files under distributions
            jarTask.destinationDir = new File(project.buildDir, 'distributions')
            // fixup the jar manifest
            jarTask.doFirst {
                boolean isSnapshot = VersionProperties.elasticsearch.endsWith("-SNAPSHOT");
                String version = VersionProperties.elasticsearch;
                if (isSnapshot) {
                    version = version.substring(0, version.length() - 9)
                }
                // this doFirst is added before the info plugin, therefore it will run
                // after the doFirst added by the info plugin, and we can override attributes
                jarTask.manifest.attributes(
                        'X-Compile-Elasticsearch-Version': version,
                        'X-Compile-Lucene-Version': VersionProperties.lucene,
                        'X-Compile-Elasticsearch-Snapshot': isSnapshot,
                        'Build-Date': ZonedDateTime.now(ZoneOffset.UTC),
                        'Build-Java-Version': project.javaVersion)
                if (jarTask.manifest.attributes.containsKey('Change') == false) {
                    logger.warn('Building without git revision id.')
                    jarTask.manifest.attributes('Change': 'N/A')
                }
            }
        }
    }

    /** Returns a closure of common configuration shared by unit and integration tests. */
    static Closure commonTestConfig(Project project) {
        return {
            jvm "${project.javaHome}/bin/java"
            parallelism System.getProperty('tests.jvms', 'auto')
            ifNoTests 'fail'
            leaveTemporary true

            // TODO: why are we not passing maxmemory to junit4?
            jvmArg '-Xmx' + System.getProperty('tests.heap.size', '512m')
            jvmArg '-Xms' + System.getProperty('tests.heap.size', '512m')
            if (JavaVersion.current().isJava7()) {
                // some tests need a large permgen, but that only exists on java 7
                jvmArg '-XX:MaxPermSize=128m'
            }
            jvmArg '-XX:MaxDirectMemorySize=512m'
            jvmArg '-XX:+HeapDumpOnOutOfMemoryError'
            File heapdumpDir = new File(project.buildDir, 'heapdump')
            heapdumpDir.mkdirs()
            jvmArg '-XX:HeapDumpPath=' + heapdumpDir
            argLine System.getProperty('tests.jvm.argline')

            // we use './temp' since this is per JVM and tests are forbidden from writing to CWD
            systemProperty 'java.io.tmpdir', './temp'
            systemProperty 'java.awt.headless', 'true'
            systemProperty 'tests.gradle', 'true'
            systemProperty 'tests.artifact', project.name
            systemProperty 'tests.task', path
            systemProperty 'tests.security.manager', 'true'
            systemProperty 'jna.nosys', 'true'
            // default test sysprop values
            systemProperty 'tests.ifNoTests', 'fail'
            // TODO: remove setting logging level via system property
            systemProperty 'tests.logger.level', 'WARN'
            for (Map.Entry<String, String> property : System.properties.entrySet()) {
                if (property.getKey().startsWith('tests.') ||
                    property.getKey().startsWith('es.')) {
                    systemProperty property.getKey(), property.getValue()
                }
            }

            // System assertions (-esa) are disabled for now because of what looks like a
            // JDK bug triggered by Groovy on JDK7. We should look at re-enabling system
            // assertions when we upgrade to a new version of Groovy (currently 2.4.4) or
            // require JDK8. See https://issues.apache.org/jira/browse/GROOVY-7528.
            enableSystemAssertions false

            testLogging {
                showNumFailuresAtEnd 25
                slowTests {
                    heartbeat 10
                    summarySize 5
                }
                stackTraceFilters {
                    // custom filters: we carefully only omit test infra noise here
                    contains '.SlaveMain.'
                    regex(/^(\s+at )(org\.junit\.)/)
                    // also includes anonymous classes inside these two:
                    regex(/^(\s+at )(com\.carrotsearch\.randomizedtesting\.RandomizedRunner)/)
                    regex(/^(\s+at )(com\.carrotsearch\.randomizedtesting\.ThreadLeakControl)/)
                    regex(/^(\s+at )(com\.carrotsearch\.randomizedtesting\.rules\.)/)
                    regex(/^(\s+at )(org\.apache\.lucene\.util\.TestRule)/)
                    regex(/^(\s+at )(org\.apache\.lucene\.util\.AbstractBeforeAfterRule)/)
                }
                if (System.getProperty('tests.class') != null && System.getProperty('tests.output') == null) {
                    // if you are debugging, you want to see the output!
                    outputMode 'always'
                } else {
                    outputMode System.getProperty('tests.output', 'onerror')
                }
            }

            balancers {
                executionTime cacheFilename: ".local-${project.version}-${name}-execution-times.log"
            }

            listeners {
                junitReport()
            }

            exclude '**/*$*.class'
        }
    }

    /** Configures the test task */
    static Task configureTest(Project project) {
        Task test = project.tasks.getByName('test')
        test.configure(commonTestConfig(project))
        test.configure {
            include '**/*Tests.class'
        }
        return test
    }

    private static configurePrecommit(Project project) {
        Task precommit = PrecommitTasks.create(project, true)
        project.check.dependsOn(precommit)
        project.test.mustRunAfter(precommit)
        project.dependencyLicenses.dependencies = project.configurations.runtime - project.configurations.provided
    }
}
