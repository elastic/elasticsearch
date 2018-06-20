package com.carrotsearch.gradle.junit4

import com.carrotsearch.ant.tasks.junit4.JUnit4
import org.gradle.api.AntBuilder
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.tasks.testing.Test

import java.util.concurrent.atomic.AtomicBoolean

class RandomizedTestingPlugin implements Plugin<Project> {

    void apply(Project project) {
        setupSeed(project)
        createTestTask(project)
        configureAnt(project.ant)
    }

    /**
     * Pins the test seed at configuration time so it isn't different on every
     * {@link RandomizedTestingTask} execution. This is useful if random
     * decisions in one run of {@linkplain RandomizedTestingTask} influence the
     * outcome of subsequent runs. Pinning the seed up front like this makes
     * the reproduction line from one run be useful on another run.
     */
    static void setupSeed(Project project) {
        if (project.rootProject.ext.has('testSeed')) {
            /* Skip this if we've already pinned the testSeed. It is important
             * that this checks the rootProject so that we know we've only ever
             * initialized one time. */
            return
        }
        String testSeed = System.getProperty('tests.seed')
        if (testSeed == null) {
            long seed = new Random(System.currentTimeMillis()).nextLong()
            testSeed = Long.toUnsignedString(seed, 16).toUpperCase(Locale.ROOT)
        }
        /* Set the testSeed on the root project first so other projects can use
         * it during initialization. */
        project.rootProject.ext.testSeed = testSeed
        project.rootProject.subprojects {
            project.ext.testSeed = testSeed
        }
    }

    static void createTestTask(Project project) {
        Test oldTestTask = project.tasks.findByPath('test')
        if (oldTestTask != null) {
            oldTestTask.enabled = false
            if (oldTestTask.getDependsOn().isEmpty() == false) {
                // we used to pass dependencies along to the new task,
                // we no longer do, so make sure nobody relies on that.
                throw new Exception("did not expect any dependencies for test task but got: ${oldTestTask.getDependsOn()}")
            }
            oldTestTask.dependsOn('utest')
        } else {
            return
        }

        RandomizedTestingTask newTestTask = project.tasks.create([
            name: 'utest',
            type: RandomizedTestingTask,
            group: JavaBasePlugin.VERIFICATION_GROUP,
            description: 'Runs unit tests with the randomized testing framework'
        ])
        newTestTask.classpath = oldTestTask.classpath
        newTestTask.testClassesDir = oldTestTask.project.sourceSets.test.output.classesDir
        newTestTask.dependsOn('testClasses')

        project.tasks.findByPath('check').dependsOn.add(newTestTask)
        // if there isn't actually a tests folder disable the task. Since we still have IT and Test in mixed folders
        // need to check by file name convention
        Set<File> testSources = project.sourceSets.test.java.getFiles().findAll { it.name.endsWith("Tests.java") }
        if (testSources.isEmpty()) {
            newTestTask.enabled = false
            project.logger.info("Found ${testSources.size()} source files, utest task will be disabled")
        } else {
            project.logger.debug("Found ${testSources.size()} source files, utest task will be enabled")
        }
    }

    static void configureAnt(AntBuilder ant) {
        ant.project.addTaskDefinition('junit4:junit4', JUnit4.class)
    }
}
