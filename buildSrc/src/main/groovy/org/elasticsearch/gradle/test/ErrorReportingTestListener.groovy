package org.elasticsearch.gradle.test

import groovy.transform.CompileStatic
import org.gradle.api.logging.Logger
import org.gradle.api.tasks.testing.TestDescriptor
import org.gradle.api.tasks.testing.TestListener
import org.gradle.api.tasks.testing.TestOutputEvent
import org.gradle.api.tasks.testing.TestOutputListener
import org.gradle.api.tasks.testing.TestResult

import java.util.concurrent.ConcurrentHashMap

@CompileStatic
class ErrorReportingTestListener implements TestOutputListener, TestListener {
    private final Logger logger
    private Map<Descriptor, List<TestOutputEvent>> eventBuffer = new ConcurrentHashMap<>()

    ErrorReportingTestListener(Logger logger) {
        this.logger = logger
    }

    @Override
    void onOutput(TestDescriptor testDescriptor, TestOutputEvent outputEvent) {
        TestDescriptor suite = testDescriptor.getParent()

        // Check if this is output from the test suite itself (e.g. afterTest or beforeTest)
        if (testDescriptor.isComposite()) {
            suite = testDescriptor
        }

        List<TestOutputEvent> events = eventBuffer.computeIfAbsent(Descriptor.of(suite)) { d -> new ArrayList<>() }
        events.add(outputEvent)
    }

    @Override
    void beforeSuite(TestDescriptor suite) {

    }

    @Override
    void afterSuite(TestDescriptor suite, TestResult result) {
        try {
            // if the test suite failed, report all captured output
            if (result.getResultType() == TestResult.ResultType.FAILURE) {
                List<TestOutputEvent> events = eventBuffer.get(Descriptor.of(suite))

                if (events != null) {
                    // It's not explicit what the threading guarantees are for TestListener method execution so we'll
                    // be explicitly safe here to avoid interleaving output from multiple test suites
                    synchronized (this) {
                        logger.lifecycle("\nSuite: {}", suite)

                        for (TestOutputEvent event : events) {
                            if (event.getDestination() == TestOutputEvent.Destination.StdOut) {
                                logger.lifecycle(event.getMessage())
                            } else {
                                logger.error(event.getMessage())
                            }
                        }
                    }
                }
            }
        } finally {
            // make sure we don't hold on to test output in memory after the suite has finished
            eventBuffer.remove(suite)
        }
    }

    @Override
    void beforeTest(TestDescriptor testDescriptor) {

    }

    @Override
    void afterTest(TestDescriptor testDescriptor, TestResult result) {

    }

    /**
     * Class for identifying test output sources. We use this rather than Gradle's {@link TestDescriptor} as we want
     * to avoid any nasty memory leak issues that come from keeping Gradle implementation types in memory. Since we
     * use this a the key for our HashMap, it's best to control the implementation as there's no guarantee that Gradle's
     * various {@link TestDescriptor} implementations reliably implement equals and hashCode.
     */
    private static class Descriptor {
        private final String name
        private final String className
        private final String parent

        private Descriptor(String name, String className, String parent) {
            this.name = name
            this.className = className
            this.parent = parent
        }

        static Descriptor of(TestDescriptor d) {
            return new Descriptor(d.name, d.className, d.parent == null ? null : d.parent.toString())
        }

        boolean equals(o) {
            if (this.is(o)) return true
            if (getClass() != o.class) return false

            Descriptor that = (Descriptor) o

            if (className != that.className) return false
            if (name != that.name) return false
            if (parent != that.parent) return false

            return true
        }

        int hashCode() {
            int result
            result = (name != null ? name.hashCode() : 0)
            result = 31 * result + (className != null ? className.hashCode() : 0)
            result = 31 * result + (parent != null ? parent.hashCode() : 0)
            return result
        }
    }
}
