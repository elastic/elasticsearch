package com.carrotsearch.gradle.junit4

import com.carrotsearch.ant.tasks.junit4.ListenersList
import com.carrotsearch.ant.tasks.junit4.listeners.AggregatedEventListener
import groovy.xml.NamespaceBuilder
import groovy.xml.NamespaceBuilderSupport
import org.apache.tools.ant.BuildException
import org.apache.tools.ant.DefaultLogger
import org.apache.tools.ant.RuntimeConfigurable
import org.apache.tools.ant.UnknownElement
import org.gradle.api.DefaultTask
import org.gradle.api.file.FileCollection
import org.gradle.api.file.FileTreeElement
import org.gradle.api.internal.tasks.options.Option
import org.gradle.api.specs.Spec
import org.gradle.api.tasks.*
import org.gradle.api.tasks.util.PatternFilterable
import org.gradle.api.tasks.util.PatternSet
import org.gradle.logging.ProgressLoggerFactory
import org.gradle.util.ConfigureUtil

import javax.inject.Inject

class RandomizedTestingTask extends DefaultTask {

    // TODO: change to "executable" to match gradle test params?
    @Optional
    @Input
    String jvm = 'java'

    @Optional
    @Input
    File workingDir = new File(project.buildDir, 'testrun' + File.separator + name)

    @Optional
    @Input
    FileCollection classpath

    @Input
    String parallelism = '1'

    @InputDirectory
    File testClassesDir

    @Optional
    @Input
    boolean haltOnFailure = true

    @Optional
    @Input
    boolean shuffleOnSlave = true

    @Optional
    @Input
    boolean enableAssertions = true

    @Optional
    @Input
    boolean enableSystemAssertions = true

    TestLoggingConfiguration testLoggingConfig = new TestLoggingConfiguration()

    BalancersConfiguration balancersConfig = new BalancersConfiguration(task: this)
    ListenersConfiguration listenersConfig = new ListenersConfiguration(task: this)

    List<String> jvmArgs = new ArrayList<>()

    @Optional
    @Input
    String argLine = null

    Map<String, String> systemProperties = new HashMap<>()
    PatternFilterable patternSet = new PatternSet()

    RandomizedTestingTask() {
        outputs.upToDateWhen {false} // randomized tests are never up to date
        listenersConfig.listeners.add(new TestProgressLogger(factory: getProgressLoggerFactory()))
        listenersConfig.listeners.add(new TestReportLogger(logger: logger, config: testLoggingConfig))
    }

    @Inject
    ProgressLoggerFactory getProgressLoggerFactory() {
        throw new UnsupportedOperationException();
    }

    void jvmArgs(Iterable<String> arguments) {
        jvmArgs.addAll(arguments)
    }

    void jvmArg(String argument) {
        jvmArgs.add(argument)
    }

    void systemProperty(String property, String value) {
        systemProperties.put(property, value)
    }

    void include(String... includes) {
        this.patternSet.include(includes);
    }

    void include(Iterable<String> includes) {
        this.patternSet.include(includes);
    }

    void include(Spec<FileTreeElement> includeSpec) {
        this.patternSet.include(includeSpec);
    }

    void include(Closure includeSpec) {
        this.patternSet.include(includeSpec);
    }

    void exclude(String... excludes) {
        this.patternSet.exclude(excludes);
    }

    void exclude(Iterable<String> excludes) {
        this.patternSet.exclude(excludes);
    }

    void exclude(Spec<FileTreeElement> excludeSpec) {
        this.patternSet.exclude(excludeSpec);
    }

    void exclude(Closure excludeSpec) {
        this.patternSet.exclude(excludeSpec);
    }

    @Input
    void testLogging(Closure closure) {
        ConfigureUtil.configure(closure, testLoggingConfig)
    }

    @Input
    void balancers(Closure closure) {
        ConfigureUtil.configure(closure, balancersConfig)
    }

    @Input
    void listeners(Closure closure) {
        ConfigureUtil.configure(closure, listenersConfig)
    }

    @Option(
            option = "tests",
            description = "Sets test class or method name to be included. This is for IDEs. Use -Dtests.class and -Dtests.method"
    )
    void setTestNameIncludePattern(String testNamePattern) {
        // This is only implemented to give support for IDEs running tests. There are 3 patterns expected:
        // * An exact test class and method
        // * An exact test class
        // * A package name prefix, ending with .*
        // There is no way to distinguish the first two without looking at classes, so we use the rule
        // that class names start with an uppercase letter...
        // TODO: this doesn't work yet, but not sure why...intellij says it is using --tests, and this work from the command line...
        String[] parts = testNamePattern.split('\\.')
        String lastPart = parts[parts.length - 1]
        String classname
        String methodname = null
        if (lastPart.equals('*') || lastPart.charAt(0).isUpperCase()) {
            // package name or class name, just pass through
            classname = testNamePattern
        } else {
            // method name, need to separate
            methodname = lastPart
            classname = testNamePattern.substring(0, testNamePattern.length() - lastPart.length() - 1)
        }
        ant.setProperty('tests.class', classname)
        if (methodname != null) {
            ant.setProperty('tests.method', methodname)
        }
    }

    // TODO: add leaveTemporary
    // TODO: add ifNoTests!

    @TaskAction
    void executeTests() {
        Map attributes = [
            jvm: jvm,
            parallelism: parallelism,
            heartbeat: testLoggingConfig.slowTests.heartbeat,
            dir: workingDir,
            tempdir: new File(workingDir, 'temp'),
            haltOnFailure: true, // we want to capture when a build failed, but will decide whether to rethrow later
            shuffleOnSlave: shuffleOnSlave
        ]

        DefaultLogger listener = null
        ByteArrayOutputStream antLoggingBuffer = null
        if (logger.isInfoEnabled() == false) {
            // in info logging, ant already outputs info level, so we see everything
            // but on errors or when debugging, we want to see info level messages
            // because junit4 emits jvm output with ant logging
            if (testLoggingConfig.outputMode == TestLoggingConfiguration.OutputMode.ALWAYS) {
                // we want all output, so just stream directly
                listener = new DefaultLogger(
                        errorPrintStream: System.err,
                        outputPrintStream: System.out,
                        messageOutputLevel: org.apache.tools.ant.Project.MSG_INFO)
            } else {
                // we want to buffer the info, and emit it if the test fails
                antLoggingBuffer = new ByteArrayOutputStream()
                PrintStream stream = new PrintStream(antLoggingBuffer, true, "UTF-8")
                listener = new DefaultLogger(
                        errorPrintStream: stream,
                        outputPrintStream: stream,
                        messageOutputLevel: org.apache.tools.ant.Project.MSG_INFO)
            }
            project.ant.project.addBuildListener(listener)
        }

        NamespaceBuilderSupport junit4 = NamespaceBuilder.newInstance(ant, 'junit4')
        try {
            junit4.junit4(attributes) {
                classpath {
                    pathElement(path: classpath.asPath)
                }
                if (enableAssertions) {
                    jvmarg(value: '-ea')
                }
                if (enableSystemAssertions) {
                    jvmarg(value: '-esa')
                }
                for (String arg : jvmArgs) {
                    jvmarg(value: arg)
                }
                if (argLine != null) {
                    jvmarg(line: argLine)
                }
                fileset(dir: testClassesDir) {
                    for (String includePattern : patternSet.getIncludes()) {
                        include(name: includePattern)
                    }
                    for (String excludePattern : patternSet.getExcludes()) {
                        exclude(name: excludePattern)
                    }
                }
                for (Map.Entry<String, String> prop : systemProperties) {
                    sysproperty key: prop.getKey(), value: prop.getValue()
                }
                makeListeners()
            }
        } catch (BuildException e) {
            if (antLoggingBuffer != null) {
                logger.error('JUnit4 test failed, ant output was:')
                logger.error(antLoggingBuffer.toString('UTF-8'))
            }
            if (haltOnFailure) {
                throw e;
            }
        }

        if (listener != null) {
            // remove the listener we added so other ant tasks dont have verbose logging!
            project.ant.project.removeBuildListener(listener)
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
    def makeListeners() {
        def context = ant.getAntXmlContext()
        def parentWrapper = context.currentWrapper()
        def parent = parentWrapper.getProxy()
        UnknownElement element = new ListenersElement(listeners: listenersConfig.listeners)
        element.setProject(context.getProject())
        element.setRealThing(logger)
        ((UnknownElement)parent).addChild(element)
        RuntimeConfigurable wrapper = new RuntimeConfigurable(element, element.getQName())
        parentWrapper.addChild(wrapper)
        return wrapper.getProxy()
    }
}
