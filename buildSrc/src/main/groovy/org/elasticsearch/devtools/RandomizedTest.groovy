package org.elasticsearch.devtools

import com.carrotsearch.ant.tasks.junit4.JUnit4
import com.carrotsearch.ant.tasks.junit4.ListenersList
import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.eventbus.Subscribe
import com.carrotsearch.ant.tasks.junit4.events.aggregated.AggregatedStartEvent
import com.carrotsearch.ant.tasks.junit4.listeners.AggregatedEventListener
import com.carrotsearch.ant.tasks.junit4.listeners.TextReport
import groovy.xml.NamespaceBuilder
import org.gradle.api.DefaultTask
import org.gradle.api.logging.Logger
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.testing.TestReport
import org.gradle.logging.ProgressLogger
import org.gradle.logging.ProgressLoggerFactory

import javax.inject.Inject

class RandomizedTest extends DefaultTask {

    static class JUnit4ProgressLogger implements AggregatedEventListener {
        private JUnit4 junit
        Logger logger
        ProgressLoggerFactory factory
        ProgressLogger progressLogger

        @Subscribe
        public void onStart(AggregatedStartEvent e) throws IOException {
            logger.info('START EVENT')
            progressLogger = factory.newOperation(JUnit4ProgressLogger.class)
            progressLogger.setDescription("Running JUnit4 " + e.getSuiteCount() + " test suites")
            progressLogger.started()
            progressLogger.progress("RUNNING TEST")
        }

        @Override
        public void setOuter(JUnit4 junit) {
            logger.info('OUTER SET')
            this.junit = junit;
        }
    }

    @Inject
    protected ProgressLoggerFactory getProgressLoggerFactory() {
        throw new UnsupportedOperationException();
    }

    @TaskAction
    void executeTests() {
        def sourceSet = ((SourceSetContainer)getProject().getProperties().get('sourceSets')).getByName('test')
        def workingDir = new File(getProject().buildDir, "run-test")
        ant.taskdef(resource: 'com/carrotsearch/junit4/antlib.xml',
                uri: 'junit4',
                classpath: getProject().configurations.testCompile.asPath)
        def junit4 = NamespaceBuilder.newInstance(ant, 'junit4')
        logger.lifecycle('RUNNING TESTS')
        junit4.junit4(
                taskName: 'junit4',
                parallelism: 8,
                dir: workingDir) {
            classpath {
                pathElement(path: sourceSet.runtimeClasspath.asPath)
            }
            jvmarg(line: '-ea -esa')
            fileset(dir: sourceSet.output.classesDir) {
                //include(name: '**/*IT.class') // temp
                include(name:'**/*Test.class')
                include(name:'**/*Tests.class')
                //exclude(name: '**/Abstract*.class')
                //exclude(name: '**/*StressTest.class')
            }
            listeners {
                junit4.'report-text'(
                        showThrowable: true,
                        showStackTraces: true,
                        showOutput: 'onerror', // TODO: change to property
                        showStatusOk: false,
                        showStatusError: true,
                        showStatusFailure: true,
                        showStatusIgnored: true,
                        showSuiteSummary: true,
                        timestamps: false
                )
                //logger.info('DELGATE: ' + delegate)
                //delegate.addConfigured(new JUnit4ProgressLogger(factory: getProgressLoggerFactory(), logger: getLogger()))
            }
        }
    }
}
