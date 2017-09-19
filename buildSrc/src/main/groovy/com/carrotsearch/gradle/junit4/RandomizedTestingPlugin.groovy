package com.carrotsearch.gradle.junit4

import com.carrotsearch.ant.tasks.junit4.JUnit4
import org.gradle.api.AntBuilder
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.tasks.TaskContainer
import org.gradle.api.tasks.testing.Test

class RandomizedTestingPlugin implements Plugin<Project> {

    void apply(Project project) {
        setupSeed(project)
        replaceTestTask(project.tasks)
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

    static void replaceTestTask(TaskContainer tasks) {
        Test oldTestTask = tasks.findByPath('test')
        if (oldTestTask == null) {
            // no test task, ok, user will use testing task on their own
            return
        }
        tasks.remove(oldTestTask)

        Map properties = [
            name: 'test',
            type: RandomizedTestingTask,
            dependsOn: oldTestTask.dependsOn,
            group: JavaBasePlugin.VERIFICATION_GROUP,
            description: 'Runs unit tests with the randomized testing framework'
        ]
        RandomizedTestingTask newTestTask = tasks.create(properties)
        newTestTask.classpath = oldTestTask.classpath
        newTestTask.testClassesDir = oldTestTask.project.sourceSets.test.output.classesDir

        // hack so check task depends on custom test
        Task checkTask = tasks.findByPath('check')
        checkTask.dependsOn.remove(oldTestTask)
        checkTask.dependsOn.add(newTestTask)
    }

    static void configureAnt(AntBuilder ant) {
        ant.project.addTaskDefinition('junit4:junit4', JUnit4.class)
    }
}
