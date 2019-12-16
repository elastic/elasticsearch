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
package org.elasticsearch.gradle.precommit


import de.thetaphi.forbiddenapis.gradle.CheckForbiddenApis
import de.thetaphi.forbiddenapis.gradle.ForbiddenApisPlugin
import org.elasticsearch.gradle.ExportElasticsearchBuildResourcesTask
import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.info.BuildParams
import org.gradle.api.JavaVersion
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.plugins.quality.Checkstyle
import org.gradle.api.tasks.TaskProvider

/**
 * Validation tasks which should be run before committing. These run before tests.
 */
class PrecommitTasks {

    /** Adds a precommit task, which depends on non-test verification tasks. */

    public static final String CHECKSTYLE_VERSION = '8.20'

    public static TaskProvider create(Project project, boolean includeDependencyLicenses) {
        project.configurations.create("forbiddenApisCliJar")
        project.dependencies {
            forbiddenApisCliJar('de.thetaphi:forbiddenapis:2.7')
        }

        Configuration jarHellConfig = project.configurations.create("jarHell")
        if (BuildParams.internal && project.path.equals(":libs:elasticsearch-core") == false) {
            // External plugins will depend on this already via transitive dependencies.
            // Internal projects are not all plugins, so make sure the check is available
            // we are not doing this for this project itself to avoid jar hell with itself
            project.dependencies {
                jarHell project.project(":libs:elasticsearch-core")
            }
        }

        List<TaskProvider> precommitTasks = [
                configureCheckstyle(project),
                configureForbiddenApisCli(project),
                project.tasks.register('forbiddenPatterns', ForbiddenPatternsTask),
                project.tasks.register('licenseHeaders', LicenseHeadersTask),
                project.tasks.register('filepermissions', FilePermissionsTask),
                configureJarHell(project, jarHellConfig),
                configureThirdPartyAudit(project),
                configureTestingConventions(project)
        ]

        // tasks with just tests don't need dependency licenses, so this flag makes adding
        // the task optional
        if (includeDependencyLicenses) {
            TaskProvider<DependencyLicensesTask> dependencyLicenses = project.tasks.register('dependencyLicenses', DependencyLicensesTask)
            precommitTasks.add(dependencyLicenses)
            // we also create the updateShas helper task that is associated with dependencyLicenses
            project.tasks.register('updateShas', UpdateShasTask) {
                it.parentTask = dependencyLicenses
            }
        }
        if (project.path != ':build-tools') {
            /*
             * Sadly, build-tools can't have logger-usage-check because that
             * would create a circular project dependency between build-tools
             * (which provides NamingConventionsCheck) and :test:logger-usage
             * which provides the logger usage check. Since the build tools
             * don't use the logger usage check because they don't have any
             * of Elaticsearch's loggers and :test:logger-usage actually does
             * use the NamingConventionsCheck we break the circular dependency
             * here.
             */
            precommitTasks.add(configureLoggerUsage(project))
        }

        // We want to get any compilation error before running the pre-commit checks.
        project.sourceSets.all { sourceSet ->
            precommitTasks.each { provider ->
                provider.configure {
                    shouldRunAfter(sourceSet.getClassesTaskName())
                }
            }
        }

        return project.tasks.register('precommit') {
            group = JavaBasePlugin.VERIFICATION_GROUP
            description = 'Runs all non-test checks.'
            dependsOn = precommitTasks
        }
    }

    static TaskProvider configureTestingConventions(Project project) {
        return project.getTasks().register("testingConventions", TestingConventionsTasks) {
            naming {
                Tests {
                    baseClass "org.apache.lucene.util.LuceneTestCase"
                }
                IT {
                    baseClass "org.elasticsearch.test.ESIntegTestCase"
                    baseClass 'org.elasticsearch.test.rest.ESRestTestCase'
                }
            }
        }
    }

    private static TaskProvider configureJarHell(Project project, Configuration jarHellConfig) {
        return project.tasks.register('jarHell', JarHellTask) { task ->
            task.classpath = project.sourceSets.test.runtimeClasspath + jarHellConfig
            task.dependsOn(jarHellConfig)
        }
    }

    private static TaskProvider configureThirdPartyAudit(Project project) {
        ExportElasticsearchBuildResourcesTask buildResources = project.tasks.getByName('buildResources')
        return project.tasks.register('thirdPartyAudit', ThirdPartyAuditTask) { task ->
            task.dependsOn(buildResources)
            task.signatureFile = buildResources.copy("forbidden/third-party-audit.txt")
            task.javaHome = BuildParams.runtimeJavaHome
            task.targetCompatibility.set(project.provider({ BuildParams.runtimeJavaVersion }))
        }
    }

    private static TaskProvider configureForbiddenApisCli(Project project) {
        project.pluginManager.apply(ForbiddenApisPlugin)
        ExportElasticsearchBuildResourcesTask buildResources = project.tasks.getByName('buildResources')
        project.tasks.withType(CheckForbiddenApis).configureEach {
            dependsOn(buildResources)
            doFirst {
                // we need to defer this configuration since we don't know the runtime java version until execution time
                targetCompatibility = BuildParams.runtimeJavaVersion.majorVersion
                if (BuildParams.runtimeJavaVersion > JavaVersion.VERSION_13) {
                    project.logger.warn(
                            "Forbidden APIs does not support Java versions past 13. Will use the signatures from 13 for {}.",
                            BuildParams.runtimeJavaVersion
                    )
                    targetCompatibility = JavaVersion.VERSION_13.majorVersion
                }
            }
            bundledSignatures = [
                    "jdk-unsafe", "jdk-deprecated", "jdk-non-portable", "jdk-system-out"
            ]
            signaturesFiles = project.files(
                    buildResources.copy("forbidden/jdk-signatures.txt"),
                    buildResources.copy("forbidden/es-all-signatures.txt")
            )
            suppressAnnotations = ['**.SuppressForbidden']
            if (name.endsWith('Test')) {
                signaturesFiles += project.files(
                        buildResources.copy("forbidden/es-test-signatures.txt"),
                        buildResources.copy("forbidden/http-signatures.txt")
                )
            } else {
                signaturesFiles += project.files(buildResources.copy("forbidden/es-server-signatures.txt"))
            }
            ext.replaceSignatureFiles = { String... names ->
                signaturesFiles = project.files(
                        names.collect { buildResources.copy("forbidden/${it}.txt") }
                )
            }
            ext.addSignatureFiles = { String... names ->
                signaturesFiles += project.files(
                        names.collect { buildResources.copy("forbidden/${it}.txt") }
                )
            }
        }
        TaskProvider forbiddenApis = project.tasks.named("forbiddenApis")
        forbiddenApis.configure {
            group = ""
        }
        return forbiddenApis
    }

    private static TaskProvider configureCheckstyle(Project project) {
        // Always copy the checkstyle configuration files to 'buildDir/checkstyle' since the resources could be located in a jar
        // file. If the resources are located in a jar, Gradle will fail when it tries to turn the URL into a file
        URL checkstyleConfUrl = PrecommitTasks.getResource("/checkstyle.xml")
        URL checkstyleSuppressionsUrl = PrecommitTasks.getResource("/checkstyle_suppressions.xml")
        File checkstyleDir = new File(project.buildDir, "checkstyle")
        File checkstyleSuppressions = new File(checkstyleDir, "checkstyle_suppressions.xml")
        File checkstyleConf = new File(checkstyleDir, "checkstyle.xml");
        TaskProvider copyCheckstyleConf = project.tasks.register("copyCheckstyleConf")

        // configure inputs and outputs so up to date works properly
        copyCheckstyleConf.configure {
            outputs.files(checkstyleSuppressions, checkstyleConf)
        }
        if ("jar".equals(checkstyleConfUrl.getProtocol())) {
            JarURLConnection jarURLConnection = (JarURLConnection) checkstyleConfUrl.openConnection()
            copyCheckstyleConf.configure {
                inputs.file(jarURLConnection.getJarFileURL())
            }
        } else if ("file".equals(checkstyleConfUrl.getProtocol())) {
            copyCheckstyleConf.configure {
                inputs.files(checkstyleConfUrl.getFile(), checkstyleSuppressionsUrl.getFile())
            }
        }

        copyCheckstyleConf.configure {
            doLast {
                checkstyleDir.mkdirs()
                // withStream will close the output stream and IOGroovyMethods#getBytes reads the InputStream fully and closes it
                new FileOutputStream(checkstyleConf).withStream {
                    it.write(checkstyleConfUrl.openStream().getBytes())
                }
                new FileOutputStream(checkstyleSuppressions).withStream {
                    it.write(checkstyleSuppressionsUrl.openStream().getBytes())
                }
            }
        }

        TaskProvider checkstyleTask = project.tasks.register('checkstyle') {
            dependsOn project.tasks.withType(Checkstyle)
        }
        // Apply the checkstyle plugin to create `checkstyleMain` and `checkstyleTest`. It only
        // creates them if there is main or test code to check and it makes `check` depend
        // on them. We also want `precommit` to depend on `checkstyle`.
        project.pluginManager.apply('checkstyle')
        project.checkstyle {
            configDir = checkstyleDir
            toolVersion = CHECKSTYLE_VERSION
        }

        project.tasks.withType(Checkstyle).configureEach { task ->
            task.dependsOn(copyCheckstyleConf)
            task.reports {
                html.enabled false
            }
        }

        return checkstyleTask
    }

    private static TaskProvider configureLoggerUsage(Project project) {
        Object dependency = BuildParams.internal ? project.project(':test:logger-usage') :
                "org.elasticsearch.test:logger-usage:${VersionProperties.elasticsearch}"

        project.configurations.create('loggerUsagePlugin')
        project.dependencies.add('loggerUsagePlugin', dependency)
        return project.tasks.register('loggerUsageCheck', LoggerUsageTask) {
            classpath = project.configurations.loggerUsagePlugin
        }
    }
}
