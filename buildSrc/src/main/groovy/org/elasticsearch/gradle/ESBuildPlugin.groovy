package org.elasticsearch.gradle

import org.elasticsearch.gradle.randomizedtesting.RandomizedTestingTask
import org.gradle.api.JavaVersion
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.TaskContainer

/**
 * Encapsulates adding all build logic and configuration for elasticsearch projects.
 */
class ESBuildPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        // Depends on Java to add
        project.pluginManager.apply('java')
        project.pluginManager.apply('org.elasticsearch.randomizedtesting')

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

        // copy the rest spec for tests because there are bugs reading the spec tests from the classpath
        Task copyRestSpec = createRestSpecHack(project)
        test.dependsOn(copyRestSpec)
        integTest.dependsOn(copyRestSpec)
        /*
         ====== PLAN ======
         - install tasks
           [x] randomized testing (apply plugin)
           [x] create testInteg task
           [] create license checker task
           [x] create forbiddenPatterns task
         - configure tasks
           [x] test and integ test common config
           [x] integ test additional/override (eg include pattern)
         */
    }

    static Task createRestSpecHack(Project project) {
        project.configurations.create('restSpec')
        project.dependencies {
            restSpec 'org.elasticsearch:rest-api-spec:3.0.0-SNAPSHOT'
        }
        Map copyRestSpecOptions = [
            type: Copy,
            dependsOn: project.configurations.restSpec.buildDependencies
        ]
        return project.task(copyRestSpecOptions, 'copyRestSpec') {
            from project.zipTree(project.configurations.restSpec.asPath)
            into project.sourceSets.test.output.classesDir
        }
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
            File heapdumpDir = new File(project.buildDir, 'heapdump')
            heapdumpDir.mkdirs()
            jvmArg '-XX:HeapDumpPath=' + heapdumpDir

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

            // System assertions (-esa) are disabled for now because of what looks like a
            // JDK bug triggered by Groovy on JDK7. We should look at re-enabling system
            // assertions when we upgrade to a new version of Groovy (currently 2.4.4) or
            // require JDK8. See https://issues.apache.org/jira/browse/GROOVY-7528.
            enableSystemAssertions false

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
                executionTime cacheFilename: ".local-$version-$name-execution-times.log"
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
