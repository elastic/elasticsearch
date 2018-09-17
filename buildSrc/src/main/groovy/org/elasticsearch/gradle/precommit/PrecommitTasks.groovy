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

import com.github.jengelman.gradle.plugins.shadow.ShadowPlugin
import org.elasticsearch.gradle.ExportElasticsearchBuildResourcesTask
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.plugins.quality.Checkstyle
/**
 * Validation tasks which should be run before committing. These run before tests.
 */
class PrecommitTasks {

    /** Adds a precommit task, which depends on non-test verification tasks. */
    public static Task create(Project project, boolean includeDependencyLicenses) {
        project.configurations.create("forbiddenApisCliJar")
        project.dependencies {
            forbiddenApisCliJar ('de.thetaphi:forbiddenapis:2.5')
        }

        List<Task> precommitTasks = [
            configureCheckstyle(project),
            configureForbiddenApisCli(project),
            configureNamingConventions(project),
            project.tasks.create('forbiddenPatterns', ForbiddenPatternsTask.class),
            project.tasks.create('licenseHeaders', LicenseHeadersTask.class),
            project.tasks.create('filepermissions', FilePermissionsTask.class),
            configureJarHell(project),
            configureThirdPartyAudit(project)
        ]

        // tasks with just tests don't need dependency licenses, so this flag makes adding
        // the task optional
        if (includeDependencyLicenses) {
            DependencyLicensesTask dependencyLicenses = project.tasks.create('dependencyLicenses', DependencyLicensesTask.class)
            precommitTasks.add(dependencyLicenses)
            // we also create the updateShas helper task that is associated with dependencyLicenses
            UpdateShasTask updateShas = project.tasks.create('updateShas', UpdateShasTask.class)
            updateShas.parentTask = dependencyLicenses
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
            precommitTasks.each { task ->
                task.shouldRunAfter(sourceSet.getClassesTaskName())
            }
        }

        return project.tasks.create([
            name: 'precommit',
            group: JavaBasePlugin.VERIFICATION_GROUP,
            description: 'Runs all non-test checks.',
            dependsOn: precommitTasks
        ])
    }

    private static Task configureJarHell(Project project) {
        Task task = project.tasks.create('jarHell', JarHellTask.class)
        task.classpath = project.sourceSets.test.runtimeClasspath
        if (project.plugins.hasPlugin(ShadowPlugin)) {
            task.classpath += project.configurations.bundle
        }
        task.dependsOn(project.sourceSets.test.classesTaskName)
        task.javaHome = project.runtimeJavaHome
        return task
    }

    private static Task configureThirdPartyAudit(Project project) {
        ThirdPartyAuditTask thirdPartyAuditTask = project.tasks.create('thirdPartyAudit', ThirdPartyAuditTask.class)
        ExportElasticsearchBuildResourcesTask buildResources = project.tasks.getByName('buildResources')
        thirdPartyAuditTask.configure {
            dependsOn(buildResources)
            signatureFile = buildResources.copy("forbidden/third-party-audit.txt")
            javaHome = project.runtimeJavaHome
            targetCompatibility = project.runtimeJavaVersion
        }
        return thirdPartyAuditTask
    }

    private static Task configureForbiddenApisCli(Project project) {
        Task forbiddenApisCli = project.tasks.create('forbiddenApis')
        project.sourceSets.all { sourceSet ->
            forbiddenApisCli.dependsOn(
                project.tasks.create(sourceSet.getTaskName('forbiddenApis', null), ForbiddenApisCliTask) {
                    ExportElasticsearchBuildResourcesTask buildResources = project.tasks.getByName('buildResources')
                    dependsOn(buildResources)
                    it.sourceSet = sourceSet
                    javaHome = project.runtimeJavaHome
                    targetCompatibility = project.compilerJavaVersion
                    bundledSignatures = [
                       "jdk-unsafe", "jdk-deprecated", "jdk-non-portable", "jdk-system-out"
                    ]
                    signaturesFiles = project.files(
                            buildResources.copy("forbidden/jdk-signatures.txt"),
                            buildResources.copy("forbidden/es-all-signatures.txt")
                    )
                    suppressAnnotations = ['**.SuppressForbidden']
                    if (sourceSet.name == 'test') {
                        signaturesFiles += project.files(
                                buildResources.copy("forbidden/es-test-signatures.txt"),
                                buildResources.copy("forbidden/http-signatures.txt")
                        )
                    } else {
                        signaturesFiles += project.files(buildResources.copy("forbidden/es-server-signatures.txt"))
                    }
                    dependsOn sourceSet.classesTaskName
                    classesDirs = sourceSet.output.classesDirs
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
            )
        }
        return forbiddenApisCli
    }

    private static Task configureCheckstyle(Project project) {
        // Always copy the checkstyle configuration files to 'buildDir/checkstyle' since the resources could be located in a jar
        // file. If the resources are located in a jar, Gradle will fail when it tries to turn the URL into a file
        URL checkstyleConfUrl = PrecommitTasks.getResource("/checkstyle.xml")
        URL checkstyleSuppressionsUrl = PrecommitTasks.getResource("/checkstyle_suppressions.xml")
        File checkstyleDir = new File(project.buildDir, "checkstyle")
        File checkstyleSuppressions = new File(checkstyleDir, "checkstyle_suppressions.xml")
        File checkstyleConf = new File(checkstyleDir, "checkstyle.xml");
        Task copyCheckstyleConf = project.tasks.create("copyCheckstyleConf")

        // configure inputs and outputs so up to date works properly
        copyCheckstyleConf.outputs.files(checkstyleSuppressions, checkstyleConf)
        if ("jar".equals(checkstyleConfUrl.getProtocol())) {
            JarURLConnection jarURLConnection = (JarURLConnection) checkstyleConfUrl.openConnection()
            copyCheckstyleConf.inputs.file(jarURLConnection.getJarFileURL())
        } else if ("file".equals(checkstyleConfUrl.getProtocol())) {
            copyCheckstyleConf.inputs.files(checkstyleConfUrl.getFile(), checkstyleSuppressionsUrl.getFile())
        }

        copyCheckstyleConf.doLast {
            checkstyleDir.mkdirs()
            // withStream will close the output stream and IOGroovyMethods#getBytes reads the InputStream fully and closes it
            new FileOutputStream(checkstyleConf).withStream {
                it.write(checkstyleConfUrl.openStream().getBytes())
            }
            new FileOutputStream(checkstyleSuppressions).withStream {
                it.write(checkstyleSuppressionsUrl.openStream().getBytes())
            }
        }

        Task checkstyleTask = project.tasks.create('checkstyle')
        // Apply the checkstyle plugin to create `checkstyleMain` and `checkstyleTest`. It only
        // creates them if there is main or test code to check and it makes `check` depend
        // on them. But we want `precommit` to depend on `checkstyle` which depends on them so
        // we have to swap them.
        project.pluginManager.apply('checkstyle')
        project.checkstyle {
            config = project.resources.text.fromFile(checkstyleConf, 'UTF-8')
            configProperties = [
                suppressions: checkstyleSuppressions
            ]
            toolVersion = '8.10.1'
        }

        project.tasks.withType(Checkstyle) { task ->
            project.tasks[JavaBasePlugin.CHECK_TASK_NAME].dependsOn.remove(task)
            checkstyleTask.dependsOn(task)
            task.dependsOn(copyCheckstyleConf)
            task.inputs.file(checkstyleSuppressions)
            task.reports {
                html.enabled false
            }
        }

        return checkstyleTask
    }

    private static Task configureNamingConventions(Project project) {
        if (project.sourceSets.findByName("test")) {
            Task namingConventionsTask = project.tasks.create('namingConventions', NamingConventionsTask)
            namingConventionsTask.javaHome = project.runtimeJavaHome
            return namingConventionsTask
        }
        return null
    }

    private static Task configureLoggerUsage(Project project) {
        project.configurations.create('loggerUsagePlugin')
        project.dependencies.add('loggerUsagePlugin',
                "org.elasticsearch.test:logger-usage:${org.elasticsearch.gradle.VersionProperties.elasticsearch}")
        return project.tasks.create('loggerUsageCheck', LoggerUsageTask.class) {
            classpath = project.configurations.loggerUsagePlugin
            javaHome = project.runtimeJavaHome
        }
    }
}
