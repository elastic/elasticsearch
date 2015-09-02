package org.elasticsearch.devtools.randomizedtesting

import com.carrotsearch.ant.tasks.junit4.JUnit4
import com.carrotsearch.ant.tasks.junit4.ListenersList
import com.carrotsearch.ant.tasks.junit4.listeners.AggregatedEventListener
import groovy.xml.NamespaceBuilder
import org.apache.tools.ant.RuntimeConfigurable
import org.apache.tools.ant.UnknownElement
import org.gradle.api.DefaultTask
import org.gradle.api.logging.Logger
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.TaskAction
import org.gradle.logging.ProgressLoggerFactory

import javax.inject.Inject

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
        def logger = getLogger()
        junit4.junit4(
                taskName: 'junit4',
                parallelism: 1,
                dir: workingDir,
                seed: '12345') {
            classpath {
                pathElement(path: sourceSet.runtimeClasspath.asPath)
            }
            jvmarg(line: '-ea -esa')
            fileset(dir: sourceSet.output.classesDir) {
                include(name: test1)
                include(name: test2)
                exclude(name: '**/*$*.class')
            }
            makeListeners(ant, factory, logger)
        }
    }

    static class ListenersElement extends UnknownElement {
        AggregatedEventListener[] listeners

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
     * the element adds the already created children.
     */
    def makeListeners(AntBuilder ant, ProgressLoggerFactory factory, Logger logger) {
        def progressLogger = new TestProgressLogger(factory: factory)
        def reportLogger = new TestReportLogger(logger: logger)
        def context = ant.getAntXmlContext()
        def parentWrapper = context.currentWrapper()
        def parent = parentWrapper.getProxy()
        UnknownElement element = new ListenersElement(listeners: [progressLogger, reportLogger])
        element.setProject(context.getProject())
        element.setRealThing(logger)
        ((UnknownElement)parent).addChild(element)
        RuntimeConfigurable wrapper = new RuntimeConfigurable(element, element.getQName())
        parentWrapper.addChild(wrapper)
        return wrapper.getProxy()
    }
}
