package org.elasticsearch.devtools.randomizedtesting

import com.carrotsearch.ant.tasks.junit4.JUnit4
import com.carrotsearch.ant.tasks.junit4.ListenersList
import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.eventbus.Subscribe
import com.carrotsearch.ant.tasks.junit4.events.aggregated.AggregatedStartEvent
import com.carrotsearch.ant.tasks.junit4.events.aggregated.AggregatedSuiteResultEvent
import com.carrotsearch.ant.tasks.junit4.listeners.AggregatedEventListener
import com.carrotsearch.ant.tasks.junit4.listeners.TextReport
import groovy.xml.NamespaceBuilder
import org.apache.tools.ant.RuntimeConfigurable
import org.apache.tools.ant.UnknownElement
import org.gradle.api.DefaultTask
import org.gradle.api.logging.Logger
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.TaskAction
import org.gradle.logging.ProgressLogger
import org.gradle.logging.ProgressLoggerFactory
import org.junit.runner.Description

import javax.inject.Inject
import java.util.concurrent.atomic.AtomicInteger

class RandomizedTestingTask extends DefaultTask {

    @Inject
    protected ProgressLoggerFactory getProgressLoggerFactory() {
        throw new UnsupportedOperationException();
    }

    @TaskAction
    void executeTests() {
        def test1 = '**/*Test.class'
        def test2 = '**/*Tests.class'
        def sourceSet = ((SourceSetContainer)getProject().getProperties().get('sourceSets')).getByName('test')
        def workingDir = new File(getProject().buildDir, "run-test")
        def factory = getProgressLoggerFactory()
        ant.getProject().addTaskDefinition('junit4:junit4', JUnit4.class)
        def junit4 = NamespaceBuilder.newInstance(ant, 'junit4')
        junit4.junit4(
                taskName: 'junit4',
                parallelism: 8,
                dir: workingDir) {
            classpath {
                pathElement(path: sourceSet.runtimeClasspath.asPath)
            }
            jvmarg(line: '-ea -esa')
            fileset(dir: sourceSet.output.classesDir) {
                include(name: test1)
                include(name: test2)
            }
            makeListeners(ant, factory, logger)
        }
    }

    static class ListenersElement extends UnknownElement {
        AggregatedEventListener[] listeners
        Logger logger

        ListenersElement() {
            super('listeners')
            setNamespace('junit4')
            setQName('listeners')
        }

        public void handleChildren(Object realThing, RuntimeConfigurable wrapper) {
            assert realThing instanceof ListenersList
            ListenersList list = (ListenersList)realThing

            for (AggregatedEventListener listener : listeners) {
                list.addConfigured(listener)
            }
        }
    }

    /**
     * Makes an ant xml element for 'listeners' just as AntBuilder would, except configuring
     * the element configures the already created children.
     */
    def makeListeners(AntBuilder ant, ProgressLoggerFactory factory, Logger realLogger) {
        def progressLogger = new JUnit4ProgressLogger(factory: factory)
        def textReport = new TextReport()
        def context = ant.getAntXmlContext();
        def parentWrapper = context.currentWrapper()
        def parent = parentWrapper.getProxy()
        UnknownElement element = new ListenersElement(listeners: [progressLogger, textReport], logger: realLogger)
        element.setProject(context.getProject())
        element.setRealThing(logger)
        ((UnknownElement)parent).addChild(element);
        RuntimeConfigurable wrapper = new RuntimeConfigurable(element, element.getQName());
        parentWrapper.addChild(wrapper)
        return wrapper.getProxy()
    }
}
