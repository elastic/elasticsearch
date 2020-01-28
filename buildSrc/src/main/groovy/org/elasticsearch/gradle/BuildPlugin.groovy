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

import com.github.jengelman.gradle.plugins.shadow.ShadowBasePlugin
import com.github.jengelman.gradle.plugins.shadow.ShadowExtension
import com.github.jengelman.gradle.plugins.shadow.ShadowJavaPlugin
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import groovy.transform.CompileStatic
import org.apache.commons.io.IOUtils
import org.elasticsearch.gradle.info.BuildParams
import org.elasticsearch.gradle.info.GlobalBuildInfoPlugin
import org.elasticsearch.gradle.info.JavaHome
import org.elasticsearch.gradle.precommit.DependencyLicensesTask
import org.elasticsearch.gradle.precommit.PrecommitTasks
import org.elasticsearch.gradle.test.ErrorReportingTestListener
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster
import org.elasticsearch.gradle.testclusters.TestClustersPlugin
import org.elasticsearch.gradle.testclusters.TestDistribution
import org.elasticsearch.gradle.tool.Boilerplate
import org.gradle.api.Action
import org.gradle.api.GradleException
import org.gradle.api.InvalidUserDataException
import org.gradle.api.JavaVersion
import org.gradle.api.NamedDomainObjectContainer
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.ModuleDependency
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.artifacts.dsl.RepositoryHandler
import org.gradle.api.artifacts.repositories.IvyArtifactRepository
import org.gradle.api.artifacts.repositories.IvyPatternRepositoryLayout
import org.gradle.api.artifacts.repositories.MavenArtifactRepository
import org.gradle.api.credentials.HttpHeaderCredentials
import org.gradle.api.execution.TaskActionListener
import org.gradle.api.execution.TaskExecutionGraph
import org.gradle.api.file.CopySpec
import org.gradle.api.plugins.BasePlugin
import org.gradle.api.plugins.BasePluginConvention
import org.gradle.api.plugins.ExtraPropertiesExtension
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin
import org.gradle.api.publish.maven.tasks.GenerateMavenPom
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.TaskProvider
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.compile.GroovyCompile
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.testing.logging.TestLoggingContainer
import org.gradle.authentication.http.HttpHeaderAuthentication
import org.gradle.external.javadoc.CoreJavadocOptions
import org.gradle.internal.jvm.Jvm
import org.gradle.language.base.plugins.LifecycleBasePlugin
import org.gradle.process.CommandLineArgumentProvider
import org.gradle.util.GradleVersion

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import static org.elasticsearch.gradle.tool.Boilerplate.maybeConfigure
import static org.elasticsearch.gradle.tool.DockerUtils.assertDockerIsAvailable
import static org.elasticsearch.gradle.tool.DockerUtils.getDockerPath

/**
 * Encapsulates build configuration for elasticsearch projects.
 */
@CompileStatic
class BuildPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        // make sure the global build info plugin is applied to the root project
        project.rootProject.pluginManager.apply(GlobalBuildInfoPlugin)

        if (project.pluginManager.hasPlugin('elasticsearch.standalone-rest-test')) {
            throw new InvalidUserDataException('elasticsearch.standalone-test, '
                    + 'elasticsearch.standalone-rest-test, and elasticsearch.build '
                    + 'are mutually exclusive')
        }
        String minimumGradleVersion = null
        InputStream is = getClass().getResourceAsStream("/minimumGradleVersion")
        try {
            minimumGradleVersion = IOUtils.toString(is, StandardCharsets.UTF_8.toString())
        } finally {
            is.close()
        }
        if (GradleVersion.current() < GradleVersion.version(minimumGradleVersion.trim())) {
            throw new GradleException(
                    "Gradle ${minimumGradleVersion}+ is required to use elasticsearch.build plugin"
            )
        }
        project.pluginManager.apply('java')
        configureConfigurations(project)
        configureJars(project) // jar config must be added before info broker
        // these plugins add lots of info to our jars
        project.pluginManager.apply('nebula.info-broker')
        project.pluginManager.apply('nebula.info-basic')
        project.pluginManager.apply('nebula.info-java')
        project.pluginManager.apply('nebula.info-scm')
        project.pluginManager.apply('nebula.info-jar')

        // apply global test task failure listener
        project.rootProject.pluginManager.apply(TestFailureReportingPlugin)

        project.getTasks().register("buildResources", ExportElasticsearchBuildResourcesTask)

        configureRepositories(project)
        project.extensions.getByType(ExtraPropertiesExtension).set('versions', VersionProperties.versions)
        configureInputNormalization(project)
        configureCompile(project)
        configureJavadoc(project)
        configureSourcesJar(project)
        configurePomGeneration(project)
        configureTestTasks(project)
        configurePrecommit(project)
        configureDependenciesInfo(project)
        configureFips140(project)
    }

    static void configureFips140(Project project) {
        // Common config when running with a FIPS-140 runtime JVM
        if (inFipsJvm()) {
            ExportElasticsearchBuildResourcesTask buildResources = project.tasks.getByName('buildResources') as ExportElasticsearchBuildResourcesTask
            File securityProperties = buildResources.copy("fips_java.security")
            File securityPolicy = buildResources.copy("fips_java.policy")
            File bcfksKeystore = buildResources.copy("cacerts.bcfks")
            // This configuration can be removed once system modules are available
            Boilerplate.maybeCreate(project.configurations, 'extraJars') {
                project.dependencies.add('extraJars', "org.bouncycastle:bc-fips:1.0.1")
                project.dependencies.add('extraJars', "org.bouncycastle:bctls-fips:1.0.9")
            }
            project.pluginManager.withPlugin("elasticsearch.testclusters") {
                NamedDomainObjectContainer<ElasticsearchCluster> testClusters = project.extensions.findByName(TestClustersPlugin.EXTENSION_NAME) as NamedDomainObjectContainer<ElasticsearchCluster>
                testClusters.all { ElasticsearchCluster cluster ->
                    for (File dep : project.getConfigurations().getByName("extraJars").getFiles()){
                        cluster.extraJarFile(dep)
                    }
                    cluster.extraConfigFile("fips_java.security", securityProperties)
                    cluster.extraConfigFile("fips_java.policy", securityPolicy)
                    cluster.extraConfigFile("cacerts.bcfks", bcfksKeystore)
                    cluster.systemProperty('java.security.properties', '=${ES_PATH_CONF}/fips_java.security')
                    cluster.systemProperty('java.security.policy', '=${ES_PATH_CONF}/fips_java.policy')
                    cluster.systemProperty('javax.net.ssl.trustStore', '${ES_PATH_CONF}/cacerts.bcfks')
                    cluster.systemProperty('javax.net.ssl.trustStorePassword', 'password')
                    cluster.systemProperty('javax.net.ssl.keyStorePassword', 'password')
                    cluster.systemProperty('javax.net.ssl.keyStoreType', 'BCFKS')
                }
            }
            project.tasks.withType(Test).configureEach { Test task ->
                task.dependsOn(buildResources)
                task.systemProperty('javax.net.ssl.trustStorePassword', 'password')
                task.systemProperty('javax.net.ssl.keyStorePassword', 'password')
                task.systemProperty('javax.net.ssl.trustStoreType', 'BCFKS')
                // Using the key==value format to override default JVM security settings and policy
                // see also: https://docs.oracle.com/javase/8/docs/technotes/guides/security/PolicyFiles.html
                task.systemProperty('java.security.properties', String.format(Locale.ROOT, "=%s", securityProperties.toString()))
                task.systemProperty('java.security.policy', String.format(Locale.ROOT, "=%s", securityPolicy.toString()))
                task.systemProperty('javax.net.ssl.trustStore', bcfksKeystore.toString())
            }

        }
    }

    static void requireDocker(final Task task) {
        final Project rootProject = task.project.rootProject
        ExtraPropertiesExtension ext = rootProject.extensions.getByType(ExtraPropertiesExtension)

        if (rootProject.hasProperty('requiresDocker') == false) {
            /*
             * This is our first time encountering a task that requires Docker. We will add an extension that will let us track the tasks
             * that register as requiring Docker. We will add a delayed execution that when the task graph is ready if any such tasks are
             * in the task graph, then we check two things:
             *  - the Docker binary is available
             *  - we can execute a Docker command that requires privileges
             *
             *  If either of these fail, we fail the build.
             */

            // check if the Docker binary exists and record its path
            final String dockerBinary = getDockerPath().orElse(null)

            final boolean buildDocker
            final String buildDockerProperty = System.getProperty("build.docker")
            if (buildDockerProperty == null) {
                buildDocker = dockerBinary != null
            } else if (buildDockerProperty == "true") {
                buildDocker = true
            } else if (buildDockerProperty == "false") {
                buildDocker = false
            } else {
                throw new IllegalArgumentException(
                        "expected build.docker to be unset or one of \"true\" or \"false\" but was [" + buildDockerProperty + "]")
            }

            ext.set('buildDocker', buildDocker)
            ext.set('requiresDocker', [])
            rootProject.gradle.taskGraph.whenReady { TaskExecutionGraph taskGraph ->
                final List<String> tasks = taskGraph.allTasks.intersect(ext.get('requiresDocker') as List<Task>).collect { "  ${it.path}".toString()}

                if (tasks.isEmpty() == false) {
                    assertDockerIsAvailable(task.project, tasks)
                }
            }
        }

        (ext.get('requiresDocker') as List<Task>).add(task)
    }

    /** Add a check before gradle execution phase which ensures java home for the given java version is set. */
    static void requireJavaHome(Task task, int version) {
        // use root project for global accounting
        Project rootProject = task.project.rootProject
        ExtraPropertiesExtension extraProperties = rootProject.extensions.extraProperties

        // hacky way (but the only way) to find if the task graph has already been populated
        boolean taskGraphReady
        try {
            rootProject.gradle.taskGraph.getAllTasks()
            taskGraphReady = true
        } catch (IllegalStateException) {
            taskGraphReady = false
        }

        if (taskGraphReady) {
            // check directly if the version is present since we are already executing
            if (BuildParams.javaVersions.find { it.version == version } == null) {
                throw new GradleException("JAVA${version}_HOME required to run task:\n${task}")
            }
        } else {
            // setup list of java versions we will check at the end of configuration time
            if (extraProperties.has('requiredJavaVersions') == false) {
                extraProperties.set('requiredJavaVersions', [:])
                rootProject.gradle.taskGraph.whenReady { TaskExecutionGraph taskGraph ->
                    List<String> messages = []
                    Map<Integer, List<Task>> requiredJavaVersions = (Map<Integer, List<Task>>) extraProperties.get('requiredJavaVersions')
                    for (Map.Entry<Integer, List<Task>> entry : requiredJavaVersions) {
                        if (BuildParams.javaVersions.any { it.version == entry.key }) {
                            continue
                        }
                        List<String> tasks = entry.value.findAll { taskGraph.hasTask(it) }.collect { "  ${it.path}".toString() }
                        if (tasks.isEmpty() == false) {
                            messages.add("JAVA${entry.key}_HOME required to run tasks:\n${tasks.join('\n')}".toString())
                        }
                    }
                    if (messages.isEmpty() == false) {
                        throw new GradleException(messages.join('\n'))
                    }
                }
            }
            Map<Integer, List<Task>> requiredJavaVersions = (Map<Integer, List<Task>>) extraProperties.get('requiredJavaVersions')
            requiredJavaVersions.putIfAbsent(version, [])
            requiredJavaVersions.get(version).add(task)
        }
    }

    /** A convenience method for getting java home for a version of java and requiring that version for the given task to execute */
    static String getJavaHome(final Task task, final int version) {
        requireJavaHome(task, version)
        JavaHome java = BuildParams.javaVersions.find { it.version == version }
        return java == null ? null : java.javaHome.absolutePath
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
        // we want to test compileOnly deps!
        project.configurations.getByName(JavaPlugin.TEST_COMPILE_CONFIGURATION_NAME).extendsFrom(project.configurations.getByName(JavaPlugin.COMPILE_ONLY_CONFIGURATION_NAME))

        // we are not shipping these jars, we act like dumb consumers of these things
        if (project.path.startsWith(':test:fixtures') || project.path == ':build-tools') {
            return
        }
        // fail on any conflicting dependency versions
        project.configurations.all({ Configuration configuration ->
            if (configuration.name.endsWith('Fixture')) {
                // just a self contained test-fixture configuration, likely transitive and hellacious
                return
            }
            configuration.resolutionStrategy {
                failOnVersionConflict()
            }
        })

        // force all dependencies added directly to compile/testCompile to be non-transitive, except for ES itself
        Closure disableTransitiveDeps = { Dependency dep ->
            if (dep instanceof ModuleDependency && !(dep instanceof ProjectDependency)
                    && dep.group.startsWith('org.elasticsearch') == false) {
                dep.transitive = false
            }
        }

        project.configurations.getByName(JavaPlugin.COMPILE_CONFIGURATION_NAME).dependencies.all(disableTransitiveDeps)
        project.configurations.getByName(JavaPlugin.TEST_COMPILE_CONFIGURATION_NAME).dependencies.all(disableTransitiveDeps)
        project.configurations.getByName(JavaPlugin.COMPILE_ONLY_CONFIGURATION_NAME).dependencies.all(disableTransitiveDeps)
    }

    /** Adds repositories used by ES dependencies */
    static void configureRepositories(Project project) {
        project.getRepositories().all { repository ->
            if (repository instanceof MavenArtifactRepository) {
                final MavenArtifactRepository maven = (MavenArtifactRepository) repository
                assertRepositoryURIIsSecure(maven.name, project.path, maven.getUrl())
                repository.getArtifactUrls().each { uri -> assertRepositoryURIIsSecure(maven.name, project.path, uri) }
            } else if (repository instanceof IvyArtifactRepository) {
                final IvyArtifactRepository ivy = (IvyArtifactRepository) repository
                assertRepositoryURIIsSecure(ivy.name, project.path, ivy.getUrl())
            }
        }
        RepositoryHandler repos = project.repositories
        if (System.getProperty('repos.mavenLocal') != null) {
            // with -Drepos.mavenLocal=true we can force checking the local .m2 repo which is
            // useful for development ie. bwc tests where we install stuff in the local repository
            // such that we don't have to pass hardcoded files to gradle
            repos.mavenLocal()
        }
        repos.jcenter()
        repos.ivy { IvyArtifactRepository repo ->
            repo.name = 'elasticsearch'
            repo.url = 'https://artifacts.elastic.co/downloads'
            repo.patternLayout { IvyPatternRepositoryLayout layout ->
                layout.artifact 'elasticsearch/[module]-[revision](-[classifier]).[ext]'
            }
            // this header is not a credential but we hack the capability to send this header to avoid polluting our download stats
            repo.credentials(HttpHeaderCredentials, { HttpHeaderCredentials creds ->
                creds.name = 'X-Elastic-No-KPI'
                creds.value = '1'
            } as Action<HttpHeaderCredentials>)
            repo.authentication.create('header', HttpHeaderAuthentication)
        }
        repos.maven { MavenArtifactRepository repo ->
            repo.name = 'elastic'
            repo.url = 'https://artifacts.elastic.co/maven'
        }
        String luceneVersion = VersionProperties.lucene
        if (luceneVersion.contains('-snapshot')) {
            // extract the revision number from the version with a regex matcher
            List<String> matches = (luceneVersion =~ /\w+-snapshot-([a-z0-9]+)/).getAt(0) as List<String>
            String revision = matches.get(1)
            repos.maven { MavenArtifactRepository repo ->
                repo.name = 'lucene-snapshots'
                repo.url = "https://s3.amazonaws.com/download.elasticsearch.org/lucenesnapshots/${revision}"
            }
        }
    }

    static void assertRepositoryURIIsSecure(final String repositoryName, final String projectPath, final URI uri) {
        if (uri != null && ["file", "https", "s3"].contains(uri.getScheme()) == false) {
            final String message = String.format(
                    Locale.ROOT,
                    "repository [%s] on project with path [%s] is not using a secure protocol for artifacts on [%s]",
                    repositoryName,
                    projectPath,
                    uri.toURL())
            throw new GradleException(message)
        }
    }

    /**Configuration generation of maven poms. */
    static void configurePomGeneration(Project project) {
        project.plugins.withType(MavenPublishPlugin).whenPluginAdded {
            TaskProvider generatePomTask = project.tasks.register("generatePom") { Task task ->
                task.dependsOn 'generatePomFileForNebulaPublication'
            }

            maybeConfigure(project.tasks, LifecycleBasePlugin.ASSEMBLE_TASK_NAME) { assemble ->
                assemble.dependsOn(generatePomTask)
            }

            project.tasks.withType(GenerateMavenPom).configureEach({ GenerateMavenPom pomTask ->
                pomTask.destination = "${project.buildDir}/distributions/${project.convention.getPlugin(BasePluginConvention).archivesBaseName}-${project.version}.pom"
            } as Action<GenerateMavenPom>)

            PublishingExtension publishing = project.extensions.getByType(PublishingExtension)

            project.pluginManager.withPlugin('com.github.johnrengelman.shadow') {
                MavenPublication publication = publishing.publications.maybeCreate('shadow', MavenPublication)
                ShadowExtension shadow = project.extensions.getByType(ShadowExtension)
                shadow.component(publication)
                // Workaround for https://github.com/johnrengelman/shadow/issues/334
                // Here we manually add any project dependencies in the "shadow" configuration to our generated POM
                publication.pom.withXml { xml ->
                    Node dependenciesNode = (xml.asNode().get('dependencies') as NodeList).get(0) as Node
                    project.configurations.getByName(ShadowBasePlugin.CONFIGURATION_NAME).allDependencies.each { dependency ->
                        if (dependency instanceof ProjectDependency) {
                            def dependencyNode = dependenciesNode.appendNode('dependency')
                            dependencyNode.appendNode('groupId', dependency.group)
                            dependencyNode.appendNode('artifactId', dependency.getDependencyProject().convention.getPlugin(BasePluginConvention).archivesBaseName)
                            dependencyNode.appendNode('version', dependency.version)
                            dependencyNode.appendNode('scope', 'compile')
                        }
                    }
                }
                generatePomTask.configure({ Task t -> t.dependsOn = ['generatePomFileForShadowPublication'] } as Action<Task>)
            }
        }
    }

    /**
     * Apply runtime classpath input normalization so that changes in JAR manifests don't break build cacheability
     */
    static void configureInputNormalization(Project project) {
        project.normalization.runtimeClasspath.ignore('META-INF/MANIFEST.MF')
    }

    /** Adds compiler settings to the project */
    static void configureCompile(Project project) {
        ExtraPropertiesExtension ext = project.extensions.getByType(ExtraPropertiesExtension)
        ext.set('compactProfile', 'full')

        project.extensions.getByType(JavaPluginExtension).sourceCompatibility = BuildParams.minimumRuntimeVersion
        project.extensions.getByType(JavaPluginExtension).targetCompatibility = BuildParams.minimumRuntimeVersion

        project.afterEvaluate {
            project.tasks.withType(JavaCompile).configureEach({ JavaCompile compileTask ->
                final JavaVersion targetCompatibilityVersion = JavaVersion.toVersion(compileTask.targetCompatibility)
                // we only fork if the Gradle JDK is not the same as the compiler JDK
                if (BuildParams.compilerJavaHome.canonicalPath == Jvm.current().javaHome.canonicalPath) {
                    compileTask.options.fork = false
                } else {
                    compileTask.options.fork = true
                    compileTask.options.forkOptions.javaHome = BuildParams.compilerJavaHome
                }
                /*
                 * -path because gradle will send in paths that don't always exist.
                 * -missing because we have tons of missing @returns and @param.
                 * -serial because we don't use java serialization.
                 */
                // don't even think about passing args with -J-xxx, oracle will ask you to submit a bug report :)
                // fail on all javac warnings
                compileTask.options.compilerArgs << '-Werror' << '-Xlint:all,-path,-serial,-options,-deprecation,-try' << '-Xdoclint:all' << '-Xdoclint:-missing'

                // either disable annotation processor completely (default) or allow to enable them if an annotation processor is explicitly defined
                if (compileTask.options.compilerArgs.contains("-processor") == false) {
                    compileTask.options.compilerArgs << '-proc:none'
                }

                compileTask.options.encoding = 'UTF-8'
                compileTask.options.incremental = true

                // TODO: use native Gradle support for --release when available (cf. https://github.com/gradle/gradle/issues/2510)
                compileTask.options.compilerArgs << '--release' << targetCompatibilityVersion.majorVersion
            } as Action<JavaCompile>)
            // also apply release flag to groovy, which is used in build-tools
            project.tasks.withType(GroovyCompile).configureEach({ GroovyCompile compileTask ->
                // we only fork if the Gradle JDK is not the same as the compiler JDK
                if (BuildParams.compilerJavaHome.canonicalPath == Jvm.current().javaHome.canonicalPath) {
                    compileTask.options.fork = false
                } else {
                    compileTask.options.fork = true
                    compileTask.options.forkOptions.javaHome = BuildParams.compilerJavaHome
                    compileTask.options.compilerArgs << '--release' << JavaVersion.toVersion(compileTask.targetCompatibility).majorVersion
                }
            } as Action<GroovyCompile>)
        }

        project.pluginManager.withPlugin('com.github.johnrengelman.shadow') {
            // Ensure that when we are compiling against the "original" JAR that we also include any "shadow" dependencies on the compile classpath
            project.configurations.getByName(JavaPlugin.API_ELEMENTS_CONFIGURATION_NAME).extendsFrom(project.configurations.getByName(ShadowBasePlugin.CONFIGURATION_NAME))
        }
    }

    static void configureJavadoc(Project project) {
        // remove compiled classes from the Javadoc classpath: http://mail.openjdk.java.net/pipermail/javadoc-dev/2018-January/000400.html
        final List<File> classes = new ArrayList<>()
        project.tasks.withType(JavaCompile).configureEach { JavaCompile javaCompile ->
            classes.add(javaCompile.destinationDir)
        }
        project.tasks.withType(Javadoc).configureEach { Javadoc javadoc ->
            // only explicitly set javadoc executable if compiler JDK is different from Gradle
            // this ensures better cacheability as setting ths input to an absolute path breaks portability
            if (Files.isSameFile(BuildParams.compilerJavaHome.toPath(), Jvm.current().getJavaHome().toPath()) == false) {
                javadoc.executable = new File(BuildParams.compilerJavaHome, 'bin/javadoc')
            }
            javadoc.classpath = javadoc.getClasspath().filter { f ->
                return classes.contains(f) == false
            }
            /*
             * Generate docs using html5 to suppress a warning from `javadoc`
             * that the default will change to html5 in the future.
             */
            (javadoc.options as CoreJavadocOptions).addBooleanOption('html5', true)
        }
        // ensure javadoc task is run with 'check'
        project.pluginManager.withPlugin('lifecycle-base') {
            project.tasks.getByName(LifecycleBasePlugin.CHECK_TASK_NAME).dependsOn(project.tasks.withType(Javadoc))
        }
        configureJavadocJar(project)
    }

    /** Adds a javadocJar task to generate a jar containing javadocs. */
    static void configureJavadocJar(Project project) {
        TaskProvider<Jar> javadocJarTask = project.tasks.register('javadocJar', Jar, { Jar jar ->
            jar.archiveClassifier.set('javadoc')
            jar.group = 'build'
            jar.description = 'Assembles a jar containing javadocs.'
            jar.from(project.tasks.named(JavaPlugin.JAVADOC_TASK_NAME))
        } as Action<Jar>)
        maybeConfigure(project.tasks, BasePlugin.ASSEMBLE_TASK_NAME) { Task t ->
            t.dependsOn(javadocJarTask)
        }
    }

    static void configureSourcesJar(Project project) {
        TaskProvider<Jar> sourcesJarTask = project.tasks.register('sourcesJar', Jar, { Jar jar ->
            jar.archiveClassifier.set('sources')
            jar.group = 'build'
            jar.description = 'Assembles a jar containing source files.'
            jar.from(project.extensions.getByType(SourceSetContainer).getByName(SourceSet.MAIN_SOURCE_SET_NAME).allSource)
        } as Action<Jar>)
        maybeConfigure(project.tasks, BasePlugin.ASSEMBLE_TASK_NAME) { Task t ->
            t.dependsOn(sourcesJarTask)
        }
    }

    /** Adds additional manifest info to jars */
    static void configureJars(Project project) {
        ExtraPropertiesExtension ext = project.extensions.getByType(ExtraPropertiesExtension)
        ext.set('licenseFile',  null)
        ext.set('noticeFile', null)
        project.tasks.withType(Jar).configureEach { Jar jarTask ->
            // we put all our distributable files under distributions
            jarTask.destinationDir = new File(project.buildDir, 'distributions')
            // fixup the jar manifest
            jarTask.doFirst {
                // this doFirst is added before the info plugin, therefore it will run
                // after the doFirst added by the info plugin, and we can override attributes
                JavaVersion compilerJavaVersion = BuildParams.compilerJavaVersion
                jarTask.manifest.attributes(
                        'Change': BuildParams.gitRevision,
                        'X-Compile-Elasticsearch-Version': VersionProperties.elasticsearch,
                        'X-Compile-Lucene-Version': VersionProperties.lucene,
                        'X-Compile-Elasticsearch-Snapshot': VersionProperties.isElasticsearchSnapshot(),
                        'Build-Date': BuildParams.buildDate,
                        'Build-Java-Version': compilerJavaVersion)
            }
        }
        // add license/notice files
        project.afterEvaluate {
            project.tasks.withType(Jar).configureEach { Jar jarTask ->
                if (ext.has('licenseFile') == false || ext.get('licenseFile') == null || ext.has('noticeFile') == false || ext.get('noticeFile') == null) {
                    throw new GradleException("Must specify license and notice file for project ${project.path}")
                }

                File licenseFile = ext.get('licenseFile') as File
                File noticeFile = ext.get('noticeFile') as File

                jarTask.metaInf { CopySpec spec ->
                    spec.from(licenseFile.parent) { CopySpec from ->
                        from.include licenseFile.name
                        from.rename { 'LICENSE.txt' }
                    }
                    spec.from(noticeFile.parent) { CopySpec from ->
                        from.include noticeFile.name
                        from.rename { 'NOTICE.txt' }
                    }
                }
            }
        }
        project.pluginManager.withPlugin('com.github.johnrengelman.shadow') {
            project.tasks.getByName(ShadowJavaPlugin.SHADOW_JAR_TASK_NAME).configure { ShadowJar shadowJar ->
                /*
                 * Replace the default "-all" classifier with null
                 * which will leave the classifier off of the file name.
                 */
                shadowJar.archiveClassifier.set((String) null)
                /*
                 * Not all cases need service files merged but it is
                 * better to be safe
                 */
                shadowJar.mergeServiceFiles()
            }
            // Add "original" classifier to the non-shadowed JAR to distinguish it from the shadow JAR
            project.tasks.getByName(JavaPlugin.JAR_TASK_NAME).configure { Jar jar ->
                jar.archiveClassifier.set('original')
            }
            // Make sure we assemble the shadow jar
            project.tasks.named(BasePlugin.ASSEMBLE_TASK_NAME).configure { Task task ->
               task.dependsOn 'shadowJar'
            }
        }
    }

    static void configureTestTasks(Project project) {
        ExtraPropertiesExtension ext = project.extensions.getByType(ExtraPropertiesExtension)

        // Default test task should run only unit tests
        maybeConfigure(project.tasks, 'test', Test) { Test task ->
            task.include '**/*Tests.class'
        }

        // none of this stuff is applicable to the `:buildSrc` project tests
        if (project.path != ':build-tools') {
            File heapdumpDir = new File(project.buildDir, 'heapdump')

            project.tasks.withType(Test).configureEach { Test test ->
                File testOutputDir = new File(test.reports.junitXml.getDestination(), "output")

                ErrorReportingTestListener listener = new ErrorReportingTestListener(test.testLogging, testOutputDir)
                test.extensions.add(ErrorReportingTestListener, 'errorReportingTestListener', listener)
                test.addTestOutputListener(listener)
                test.addTestListener(listener)

                /*
                 * We use lazy-evaluated strings in order to configure system properties whose value will not be known until
                 * execution time (e.g. cluster port numbers). Adding these via the normal DSL doesn't work as these get treated
                 * as task inputs and therefore Gradle attempts to snapshot them before/after task execution. This fails due
                 * to the GStrings containing references to non-serializable objects.
                 *
                 * We bypass this by instead passing this system properties vi a CommandLineArgumentProvider. This has the added
                 * side-effect that these properties are NOT treated as inputs, therefore they don't influence things like the
                 * build cache key or up to date checking.
                 */
                SystemPropertyCommandLineArgumentProvider nonInputProperties = new SystemPropertyCommandLineArgumentProvider()

                test.doFirst {
                    project.mkdir(testOutputDir)
                    project.mkdir(heapdumpDir)
                    project.mkdir(test.workingDir)

                    //TODO remove once jvm.options are added to test system properties
                    test.systemProperty ('java.locale.providers','SPI,COMPAT')
                }
                if (inFipsJvm()) {
                    project.dependencies.add('testRuntimeOnly', "org.bouncycastle:bc-fips:1.0.1")
                    project.dependencies.add('testRuntimeOnly', "org.bouncycastle:bctls-fips:1.0.9")
                }
                test.jvmArgumentProviders.add(nonInputProperties)
                test.extensions.add('nonInputProperties', nonInputProperties)

                test.workingDir = project.file("${project.buildDir}/testrun/${test.name}")
                test.maxParallelForks = System.getProperty('tests.jvms', BuildParams.defaultParallel.toString()) as Integer

                test.exclude '**/*$*.class'

                test.jvmArgs "-Xmx${System.getProperty('tests.heap.size', '512m')}",
                        "-Xms${System.getProperty('tests.heap.size', '512m')}",
                        '--illegal-access=warn',
                        '-XX:+HeapDumpOnOutOfMemoryError'

                test.jvmArgumentProviders.add({ ["-XX:HeapDumpPath=$heapdumpDir"] } as CommandLineArgumentProvider)

                if (System.getProperty('tests.jvm.argline')) {
                    test.jvmArgs System.getProperty('tests.jvm.argline').split(" ")
                }

                if (Boolean.parseBoolean(System.getProperty('tests.asserts', 'true'))) {
                    test.jvmArgs '-ea', '-esa'
                }

                // we use './temp' since this is per JVM and tests are forbidden from writing to CWD
                test.systemProperties 'java.io.tmpdir': './temp',
                        'java.awt.headless': 'true',
                        'tests.gradle': 'true',
                        'tests.artifact': project.name,
                        'tests.task': test.path,
                        'tests.security.manager': 'true',
                        'jna.nosys': 'true'


                // ignore changing test seed when build is passed -Dignore.tests.seed for cacheability experimentation
                if (System.getProperty('ignore.tests.seed') != null) {
                    nonInputProperties.systemProperty('tests.seed', BuildParams.testSeed)
                } else {
                    test.systemProperty('tests.seed', BuildParams.testSeed)
                }

                // don't track these as inputs since they contain absolute paths and break cache relocatability
                nonInputProperties.systemProperty('gradle.dist.lib', new File(project.class.location.toURI()).parent)
                nonInputProperties.systemProperty('gradle.worker.jar', "${project.gradle.getGradleUserHomeDir()}/caches/${project.gradle.gradleVersion}/workerMain/gradle-worker.jar")
                nonInputProperties.systemProperty('gradle.user.home', project.gradle.getGradleUserHomeDir())

                nonInputProperties.systemProperty('compiler.java', "${-> BuildParams.compilerJavaVersion.majorVersion}")

                // TODO: remove setting logging level via system property
                test.systemProperty 'tests.logger.level', 'WARN'
                System.getProperties().each { key, value ->
                    if ((key.toString().startsWith('tests.') || key.toString().startsWith('es.'))) {
                        test.systemProperty key.toString(), value
                    }
                }

                // TODO: remove this once ctx isn't added to update script params in 7.0
                test.systemProperty 'es.scripting.update.ctx_in_params', 'false'

                // TODO: remove this property in 8.0
                test.systemProperty 'es.search.rewrite_sort', 'true'

                // TODO: remove this once cname is prepended to transport.publish_address by default in 8.0
                test.systemProperty 'es.transport.cname_in_publish_address', 'true'

                // Set netty system properties to the properties we configure in jvm.options
                test.systemProperty('io.netty.noUnsafe', 'true')
                test.systemProperty('io.netty.noKeySetOptimization', 'true')
                test.systemProperty('io.netty.recycler.maxCapacityPerThread', '0')

                test.testLogging { TestLoggingContainer logging ->
                    logging.showExceptions = true
                    logging.showCauses = true
                    logging.exceptionFormat = 'full'
                }

                if (OS.current().equals(OS.WINDOWS) && System.getProperty('tests.timeoutSuite') == null) {
                    // override the suite timeout to 30 mins for windows, because it has the most inefficient filesystem known to man
                    test.systemProperty 'tests.timeoutSuite', '1800000!'
                }

                /*
                 *  If this project builds a shadow JAR than any unit tests should test against that artifact instead of
                 *  compiled class output and dependency jars. This better emulates the runtime environment of consumers.
                 */
                project.pluginManager.withPlugin('com.github.johnrengelman.shadow') {
                    // Remove output class files and any other dependencies from the test classpath, since the shadow JAR includes these
                    test.classpath -= project.extensions.getByType(SourceSetContainer).getByName(SourceSet.MAIN_SOURCE_SET_NAME).runtimeClasspath
                    // Add any "shadow" dependencies. These are dependencies that are *not* bundled into the shadow JAR
                    test.classpath += project.configurations.getByName(ShadowBasePlugin.CONFIGURATION_NAME)
                    // Add the shadow JAR artifact itself
                    test.classpath += project.files(project.tasks.named('shadowJar'))
                }
            }
        }
    }

    private static configurePrecommit(Project project) {
        TaskProvider precommit = PrecommitTasks.create(project, true)
        project.tasks.named(LifecycleBasePlugin.CHECK_TASK_NAME).configure { it.dependsOn(precommit) }
        project.tasks.named(JavaPlugin.TEST_TASK_NAME).configure { it.mustRunAfter(precommit) }
        // only require dependency licenses for non-elasticsearch deps
        project.tasks.withType(DependencyLicensesTask).named('dependencyLicenses').configure {
            it.dependencies = project.configurations.getByName(JavaPlugin.RUNTIME_CLASSPATH_CONFIGURATION_NAME).fileCollection { Dependency dependency ->
                dependency.group.startsWith('org.elasticsearch') == false
            } - project.configurations.getByName(JavaPlugin.COMPILE_ONLY_CONFIGURATION_NAME)
        }
    }

    private static configureDependenciesInfo(Project project) {
        project.tasks.register("dependenciesInfo", DependenciesInfoTask, { DependenciesInfoTask task ->
            task.runtimeConfiguration = project.configurations.getByName(JavaPlugin.RUNTIME_CLASSPATH_CONFIGURATION_NAME)
            task.compileOnlyConfiguration = project.configurations.getByName(JavaPlugin.COMPILE_ONLY_CONFIGURATION_NAME)
            task.getConventionMapping().map('mappings') {
                (project.tasks.getByName('dependencyLicenses') as DependencyLicensesTask).mappings
            }
        } as Action<DependenciesInfoTask>)
    }

    private static class TestFailureReportingPlugin implements Plugin<Project> {
        @Override
        void apply(Project project) {
            if (project != project.rootProject) {
                throw new IllegalStateException("${this.class.getName()} can only be applied to the root project.")
            }

            project.gradle.addListener(new TaskActionListener() {
                @Override
                void beforeActions(Task task) {

                }

                @Override
                void afterActions(Task task) {
                    if (task instanceof Test) {
                        ErrorReportingTestListener listener = task.extensions.findByType(ErrorReportingTestListener)
                        if (listener != null && listener.getFailedTests().size() > 0) {
                            task.logger.lifecycle("\nTests with failures:")
                            listener.getFailedTests().each {
                                task.logger.lifecycle(" - ${it.getFullName()}")
                            }
                        }
                    }
                }
            })
        }
    }

    private static inFipsJvm(){
        return Boolean.parseBoolean(System.getProperty("tests.fips.enabled"));
    }
}
