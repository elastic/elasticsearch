package com.carrotsearch.gradle.junit4

import com.carrotsearch.ant.tasks.junit4.JUnit4
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.UnknownTaskException
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.tasks.TaskContainer
import org.gradle.api.tasks.TaskProvider
import org.gradle.api.tasks.testing.Test

class RandomizedTestingPlugin implements Plugin<Project> {

    void apply(Project project) {
        setupSeed(project)
        replaceTestTask(project.tasks)
        configureAnt(project.ant)
        configureSanityCheck(project)
    }

    private static void configureSanityCheck(Project project) {
        // Check the task graph to confirm tasks were indeed replaced
        // https://github.com/elastic/elasticsearch/issues/31324
        project.rootProject.getGradle().getTaskGraph().whenReady {
            Task test = project.getTasks().findByName("test")
            if (test != null && (test instanceof RandomizedTestingTask) == false) {
                throw new IllegalStateException("Test task was not replaced in project ${project.path}. Found ${test.getClass()}")
            }
        }
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
        // Gradle 4.8 introduced lazy tasks, thus we deal both with the `test` task as well as it's provider
        // https://github.com/gradle/gradle/issues/5730#issuecomment-398822153
        // since we can't be sure if the task was ever realized, we remove both the provider and the task
        TaskProvider<Test> oldTestProvider
        try {
            oldTestProvider = tasks.named('test')
        } catch (UnknownTaskException unused) {
            // no test task, ok, user will use testing task on their own
            return
        }
        Test oldTestTask = oldTestProvider.get()

        // we still have to use replace here despite the remove above because the task container knows about the provider
        // by the same name
        RandomizedTestingTask newTestTask = tasks.replace('test', RandomizedTestingTask)
        newTestTask.configure{
            group =  JavaBasePlugin.VERIFICATION_GROUP
            description = 'Runs unit tests with the randomized testing framework'
            dependsOn oldTestTask.dependsOn, 'testClasses'
            classpath = oldTestTask.classpath
            testClassesDirs = oldTestTask.project.sourceSets.test.output.classesDirs
        }

        // hack so check task depends on custom test
        Task checkTask = tasks.getByName('check')
        checkTask.dependsOn.remove(oldTestProvider)
        checkTask.dependsOn.remove(oldTestTask)
        checkTask.dependsOn.add(newTestTask)
    }

    static void configureAnt(AntBuilder ant) {
        ant.project.addTaskDefinition('junit4:junit4', JUnit4.class)
    }
}
