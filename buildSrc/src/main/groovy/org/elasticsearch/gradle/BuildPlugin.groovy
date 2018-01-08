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

import com.carrotsearch.gradle.junit4.RandomizedTestingTask
import nebula.plugin.extraconfigurations.ProvidedBasePlugin
import org.apache.tools.ant.taskdefs.condition.Os
import org.eclipse.jgit.lib.Constants
import org.eclipse.jgit.lib.RepositoryBuilder
import org.elasticsearch.gradle.precommit.PrecommitTasks
import org.gradle.api.GradleException
import org.gradle.api.InvalidUserDataException
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
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin
import org.gradle.api.publish.maven.tasks.GenerateMavenPom
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.javadoc.Javadoc
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
        if (project.pluginManager.hasPlugin('elasticsearch.standalone-rest-test')) {
              throw new InvalidUserDataException('elasticsearch.standalone-test, '
                + 'elasticearch.standalone-rest-test, and elasticsearch.build '
                + 'are mutually exclusive')
        }
        project.pluginManager.apply('java')
        project.pluginManager.apply('carrotsearch.randomized-testing')
        // these plugins add lots of info to our jars
        configureJars(project) // jar config must be added before info broker
        project.pluginManager.apply('nebula.info-broker')
        project.pluginManager.apply('nebula.info-basic')
        project.pluginManager.apply('nebula.info-java')
        project.pluginManager.apply('nebula.info-scm')
        project.pluginManager.apply('nebula.info-jar')
        project.pluginManager.apply(ProvidedBasePlugin)

        globalBuildInfo(project)
        configureRepositories(project)
        configureConfigurations(project)
        project.ext.versions = VersionProperties.versions
        configureCompile(project)
        configureJavadoc(project)
        configureSourcesJar(project)
        configurePomGeneration(project)

        configureTest(project)
        configurePrecommit(project)
        configureDependenciesInfo(project)
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
            println "  Random Testing Seed   : ${project.testSeed}"

            // enforce Gradle version
            final GradleVersion currentGradleVersion = GradleVersion.current();

            final GradleVersion minGradle = GradleVersion.version('4.3')
            if (currentGradleVersion < minGradle) {
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
            if (System.getProperty("idea.active") != null || System.getProperty("eclipse.launcher") != null) {
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
        ByteArrayOutputStream stdout = new ByteArrayOutputStream()
        ByteArrayOutputStream stderr = new ByteArrayOutputStream()
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            // gradle/groovy does not properly escape the double quote for windows
            script = script.replace('"', '\\"')
        }
        File jrunscriptPath = new File(javaHome, 'bin/jrunscript')
        ExecResult result = project.exec {
            executable = jrunscriptPath
            args '-e', script
            standardOutput = stdout
            errorOutput = stderr
            ignoreExitValue = true
        }
        if (result.exitValue != 0) {
            project.logger.error("STDOUT:")
            stdout.toString('UTF-8').eachLine { line -> project.logger.error(line) }
            project.logger.error("STDERR:")
            stderr.toString('UTF-8').eachLine { line -> project.logger.error(line) }
            result.rethrowFailure()
        }
        return stdout.toString('UTF-8').trim()
    }

    /** Return the configuration name used for finding transitive deps of the given dependency. */
    private static String transitiveDepConfigName(String groupId, String artifactId, String version) {
        return "_transitive_${groupId}_${artifactId}_${version}"
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
    }

    /** Adds repositories used by ES dependencies */
    static void configureRepositories(Project project) {
        RepositoryHandler repos = project.repositories
        if (System.getProperty("repos.mavenlocal") != null) {
            // with -Drepos.mavenlocal=true we can force checking the local .m2 repo which is
            // useful for development ie. bwc tests where we install stuff in the local repository
            // such that we don't have to pass hardcoded files to gradle
            repos.mavenLocal()
        }
        repos.mavenCentral()
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

    /**
     * Returns a closure which can be used with a MavenPom for fixing problems with gradle generated poms.
     *
     * <ul>
     *     <li>Remove transitive dependencies. We currently exclude all artifacts explicitly instead of using wildcards
     *         as Ivy incorrectly translates POMs with * excludes to Ivy XML with * excludes which results in the main artifact
     *         being excluded as well (see https://issues.apache.org/jira/browse/IVY-1531). Note that Gradle 2.14+ automatically
     *         translates non-transitive dependencies to * excludes. We should revisit this when upgrading Gradle.</li>
     *     <li>Set compile time deps back to compile from runtime (known issue with maven-publish plugin)</li>
     * </ul>
     */
    private static Closure fixupDependencies(Project project) {
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

                // fix deps incorrectly marked as runtime back to compile time deps
                // see https://discuss.gradle.org/t/maven-publish-plugin-generated-pom-making-dependency-scope-runtime/7494/4
                boolean isCompileDep = project.configurations.compile.allDependencies.find { dep ->
                    dep.name == depNode.artifactId.text()
                }
                if (depNode.scope.text() == 'runtime' && isCompileDep) {
                    depNode.scope*.value = 'compile'
                }

                // remove any exclusions added by gradle, they contain wildcards and systems like ivy have bugs with wildcards
                // see https://github.com/elastic/elasticsearch/issues/24490
                NodeList exclusionsNode = depNode.get('exclusions')
                if (exclusionsNode.size() > 0) {
                    depNode.remove(exclusionsNode.get(0))
                }

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

                // we now know we have something to exclude, so add exclusions for all artifacts except the main one
                Node exclusions = depNode.appendNode('exclusions')
                for (ResolvedArtifact artifact : artifacts) {
                    ModuleVersionIdentifier moduleVersionIdentifier = artifact.moduleVersion.id;
                    String depGroupId = moduleVersionIdentifier.group
                    String depArtifactId = moduleVersionIdentifier.name
                    // add exclusions for all artifacts except the main one
                    if (depGroupId != groupId || depArtifactId != artifactId) {
                        Node exclusion = exclusions.appendNode('exclusion')
                        exclusion.appendNode('groupId', depGroupId)
                        exclusion.appendNode('artifactId', depArtifactId)
                    }
                }
            }
        }
    }

    /**Configuration generation of maven poms. */
    public static void configurePomGeneration(Project project) {
        project.plugins.withType(MavenPublishPlugin.class).whenPluginAdded {
            project.publishing {
                publications {
                    all { MavenPublication publication -> // we only deal with maven
                        // add exclusions to the pom directly, for each of the transitive deps of this project's deps
                        publication.pom.withXml(fixupDependencies(project))
                    }
                }
            }

            project.tasks.withType(GenerateMavenPom.class) { GenerateMavenPom t ->
                // place the pom next to the jar it is for
                t.destination = new File(project.buildDir, "distributions/${project.archivesBaseName}-${project.version}.pom")
                // build poms with assemble (if the assemble task exists)
                Task assemble = project.tasks.findByName('assemble')
                if (assemble) {
                    assemble.dependsOn(t)
                }
            }
        }
    }

    /** Adds compiler settings to the project */
    static void configureCompile(Project project) {
        if (project.javaVersion < JavaVersion.VERSION_1_10) {
            project.ext.compactProfile = 'compact3'
        } else {
            project.ext.compactProfile = 'full'
        }
        project.afterEvaluate {
            project.tasks.withType(JavaCompile) {
                File gradleJavaHome = Jvm.current().javaHome
                // we fork because compiling lots of different classes in a shared jvm can eventually trigger GC overhead limitations
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
                // fail on all javac warnings
                options.compilerArgs << '-Werror' << '-Xlint:all,-path,-serial,-options,-deprecation' << '-Xdoclint:all' << '-Xdoclint:-missing'

                // either disable annotation processor completely (default) or allow to enable them if an annotation processor is explicitly defined
                if (options.compilerArgs.contains("-processor") == false) {
                    options.compilerArgs << '-proc:none'
                }

                options.encoding = 'UTF-8'
                options.incremental = true

                if (project.javaVersion == JavaVersion.VERSION_1_9) {
                    // hack until gradle supports java 9's new "--release" arg
                    assert minimumJava == JavaVersion.VERSION_1_8
                    options.compilerArgs << '--release' << '8'
                }
            }
        }
    }

    static void configureJavadoc(Project project) {
        project.tasks.withType(Javadoc) {
            executable = new File(project.javaHome, 'bin/javadoc')
        }
        configureJavadocJar(project)
        if (project.javaVersion == JavaVersion.VERSION_1_10) {
            project.tasks.withType(Javadoc) { it.enabled = false }
            project.tasks.getByName('javadocJar').each { it.enabled = false }
        }
    }

    /** Adds a javadocJar task to generate a jar containing javadocs. */
    static void configureJavadocJar(Project project) {
        Jar javadocJarTask = project.task('javadocJar', type: Jar)
        javadocJarTask.classifier = 'javadoc'
        javadocJarTask.group = 'build'
        javadocJarTask.description = 'Assembles a jar containing javadocs.'
        javadocJarTask.from(project.tasks.getByName(JavaPlugin.JAVADOC_TASK_NAME))
        project.assemble.dependsOn(javadocJarTask)
    }

    static void configureSourcesJar(Project project) {
        Jar sourcesJarTask = project.task('sourcesJar', type: Jar)
        sourcesJarTask.classifier = 'sources'
        sourcesJarTask.group = 'build'
        sourcesJarTask.description = 'Assembles a jar containing source files.'
        sourcesJarTask.from(project.sourceSets.main.allSource)
        project.assemble.dependsOn(sourcesJarTask)
    }

    /** Adds additional manifest info to jars */
    static void configureJars(Project project) {
        project.ext.licenseFile = null
        project.ext.noticeFile = null
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
                    jarTask.manifest.attributes('Change': 'Unknown')
                } else {
                    /*
                     * The info-scm plugin assumes that if GIT_COMMIT is set it was set by Jenkins to the commit hash for this build.
                     * However, that assumption is wrong as this build could be a sub-build of another Jenkins build for which GIT_COMMIT
                     * is the commit hash for that build. Therefore, if GIT_COMMIT is set we calculate the commit hash ourselves.
                     */
                    if (System.getenv("GIT_COMMIT") != null) {
                        final String hash = new RepositoryBuilder().findGitDir(project.buildDir).build().resolve(Constants.HEAD).name
                        final String shortHash = hash?.substring(0, 7)
                        jarTask.manifest.attributes('Change': shortHash)
                    }
                }
            }
            // add license/notice files
            project.afterEvaluate {
                if (project.licenseFile == null || project.noticeFile == null) {
                    throw new GradleException("Must specify license and notice file for project ${project.path}")
                }
                jarTask.into('META-INF') {
                    from(project.licenseFile.parent) {
                        include project.licenseFile.name
                    }
                    from(project.noticeFile.parent) {
                        include project.noticeFile.name
                    }
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
            onNonEmptyWorkDirectory 'wipe'
            leaveTemporary true

            // TODO: why are we not passing maxmemory to junit4?
            jvmArg '-Xmx' + System.getProperty('tests.heap.size', '512m')
            jvmArg '-Xms' + System.getProperty('tests.heap.size', '512m')
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
                    if (property.getKey().equals('tests.seed')) {
                        /* The seed is already set on the project so we
                         * shouldn't attempt to override it. */
                        continue;
                    }
                    systemProperty property.getKey(), property.getValue()
                }
            }

            boolean assertionsEnabled = Boolean.parseBoolean(System.getProperty('tests.asserts', 'true'))
            enableSystemAssertions assertionsEnabled
            enableAssertions assertionsEnabled

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
        RandomizedTestingTask test = project.tasks.getByName('test')
        test.configure(commonTestConfig(project))
        test.configure {
            include '**/*Tests.class'
        }

        // Add a method to create additional unit tests for a project, which will share the same
        // randomized testing setup, but by default run no tests.
        project.extensions.add('additionalTest', { String name, Closure config ->
            RandomizedTestingTask additionalTest = project.tasks.create(name, RandomizedTestingTask.class)
            additionalTest.classpath = test.classpath
            additionalTest.testClassesDir = test.testClassesDir
            additionalTest.configure(commonTestConfig(project))
            additionalTest.configure(config)
            test.dependsOn(additionalTest)
        });
        return test
    }

    private static configurePrecommit(Project project) {
        Task precommit = PrecommitTasks.create(project, true)
        project.check.dependsOn(precommit)
        project.test.mustRunAfter(precommit)
        project.dependencyLicenses.dependencies = project.configurations.runtime - project.configurations.provided
    }

    private static configureDependenciesInfo(Project project) {
        Task deps = project.tasks.create("dependenciesInfo", DependenciesInfoTask.class)
        deps.dependencies = project.configurations.compile.allDependencies
    }
}
