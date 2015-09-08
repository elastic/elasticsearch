package org.elasticsearch.devtools

import org.elasticsearch.devtools.randomizedtesting.RandomizedTestingTask
import org.gradle.api.JavaVersion
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.tasks.TaskContainer

/**
 * Encapsulates adding all build logic and configuration for elasticsearch projects.
 */
class ElasticsearchBuildPlugin implements Plugin<Project> {
    void apply(Project project) {
        // Depends on Java to add
        project.pluginManager.apply('java')
        project.pluginManager.apply('org.elasticsearch.devtools.randomizedtesting')

        Closure testConfig = createSharedTestConfig(project)
        Task test = configureTest(project.tasks, testConfig)
        Task integTest = configureIntegTest(project.tasks, test, testConfig)

        List<Task> precommitTasks = new ArrayList<>()
        precommitTasks.add(configureForbiddenPatterns(project.tasks))

        Map precommitOptions = [
            name: 'precommit',
            group: JavaBasePlugin.VERIFICATION_GROUP,
            description: 'Runs all non test checks, useful for running before committing',
            dependsOn: precommitTasks
        ]
        Task precommit = project.tasks.create(precommitOptions)
        test.mustRunAfter(precommit)
        integTest.mustRunAfter(precommit)
        project.tasks.getByName('check').dependsOn(precommit, integTest)
        /*
         ====== PLAN ======
         - install tasks
           [] randomized testing (apply plugin)
           [] create testInteg task
           [] create license checker task
           [x] create forbiddenPatterns task
         - configure tasks
           [] test and integ test common config
           [] integ test additional/override (eg include pattern)
         */
    }

    static Closure createSharedTestConfig(Project project) {
        return {
            jvm System.getenv('JAVA_HOME') + File.separator + 'bin' + File.separator + 'java'
            parallelism System.getProperty('tests.jvms', 'auto')

            // TODO: why are we not passing maxmemory to junit4?
            jvmArg '-Xmx' + System.getProperty('tests.heap.size', '512m')
            jvmArg '-Xms' + System.getProperty('tests.heap.size', '512m')
            if (JavaVersion.current().isJava7()) {
                // some tests need a large permgen, but that only exists on java 7
                jvmArg '-XX:MaxPermSize=128m'
            }
            jvmArg '-XX:MaxDirectMemorySize=512m'
            jvmArg '-XX:+HeapDumpOnOutOfMemoryError'
            // TODO: need to create this dir?
            jvmArg '-XX:HeapDumpPath=' + new File(project.buildDir, 'heapdump')

            // we use './temp' since this is per JVM and tests are forbidden from writing to CWD
            sysProp 'java.io.tmpdir', './temp'
            sysProp 'java.awt.headless', 'true'
            sysProp 'tests.task', path
            sysProp 'tests.security.manager', 'true'
            // default test sysprop values
            sysProp 'tests.ifNoTests', 'fail'
            sysProp 'es.logger.level', 'ERROR'
            copySysPropPrefix 'tests.'
            copySysPropPrefix 'es.'

            testLogging {
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
            }

            balancers {
                def version = project.property('version')
                def taskName = task.name
                executionTime cacheFilename: ".local-$version-$taskName-execution-times.log"
            }

            exclude '**/*$*.class'
        }
    }

    static Task configureTest(TaskContainer tasks, Closure testConfig) {
        Task test = tasks.getByName('test')
        test.configure(testConfig)
        test.configure {
            include '**/*Tests.class'
        }
        return test
    }

    static Task configureIntegTest(TaskContainer tasks, Task test, Closure testConfig) {
        Map integTestOptions = [
            'name': 'integTest',
            'type': RandomizedTestingTask,
            'group': JavaBasePlugin.VERIFICATION_GROUP,
            'description': 'Tests integration of elasticsearch components',
            'dependsOn': test.dependsOn
        ]
        Task integTest = tasks.create(integTestOptions)
        integTest.mustRunAfter(test)
        integTest.configure(testConfig)
        integTest.configure {
            include '**/*IT.class'
        }
        return integTest
    }

    static Task configureForbiddenPatterns(TaskContainer tasks) {
        def options = [
            'name': 'forbiddenPatterns',
            'type': ForbiddenPatternsTask,
            'description': 'Checks source files for invalid patterns like nocommits or tabs',
        ]
        return tasks.create(options) {
            rule name: 'nocommit', pattern: /nocommit/
            rule name: 'tab', pattern: /\t/
        }
    }
}
