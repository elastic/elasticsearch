package org.elasticsearch.gradle.test;

import org.codehaus.groovy.runtime.DefaultGroovyMethods;
import org.gradle.api.tasks.testing.TestDescriptor;
import org.gradle.api.tasks.testing.TestListener;
import org.gradle.api.tasks.testing.TestOutputEvent;
import org.gradle.api.tasks.testing.TestOutputListener;
import org.gradle.api.tasks.testing.TestResult;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ErrorReportingTestListener implements TestOutputListener, TestListener {
    private Map<Descriptor, List<TestOutputEvent>> eventBuffer = new ConcurrentHashMap<>();

    @Override
    public void onOutput(TestDescriptor testDescriptor, TestOutputEvent outputEvent) {
        TestDescriptor suite = testDescriptor.getParent();

        // Check if this is output from the test suite itself (e.g. afterTest or beforeTest)
        if (testDescriptor.isComposite()) {
            suite = testDescriptor;
        }


        List<TestOutputEvent> events = eventBuffer.computeIfAbsent(Descriptor.of(suite), d -> new ArrayList<>());
        events.add(outputEvent);
    }

    @Override
    public void beforeSuite(TestDescriptor suite) {

    }

    @Override
    public void afterSuite(final TestDescriptor suite, TestResult result) {
        try {
            // if the test suite failed, report all captured output
            if (result.getResultType().equals(TestResult.ResultType.FAILURE)) {
                List<TestOutputEvent> events = eventBuffer.get(Descriptor.of(suite));

                if (events != null) {
                    // It's not explicit what the threading guarantees are for TestListener method execution so we'll
                    // be explicitly safe here to avoid interleaving output from multiple test suites
                    synchronized (this) {
                        System.out.println("\nSuite: " + suite);

                        for (TestOutputEvent event : events) {
                            log(event.getMessage(), event.getDestination());

                        }

                    }

                }

            }

        } finally {
            // make sure we don't hold on to test output in memory after the suite has finished
            eventBuffer.remove(Descriptor.of(suite));
        }

    }

    @Override
    public void beforeTest(TestDescriptor testDescriptor) {

    }

    @Override
    public void afterTest(TestDescriptor testDescriptor, TestResult result) {

    }

    private static void log(String message, TestOutputEvent.Destination destination) {
        PrintStream out;
        String prefix;

        if (destination == TestOutputEvent.Destination.StdOut) {
            out = System.out;
            prefix = "  1> ";
        } else {
            out = System.err;
            prefix = "  2> ";
        }

        if (message.equals("\n")) {
            out.print(message);
        } else {
            out.print(prefix);
            out.print(message);
        }
    }

    /**
     * Class for identifying test output sources. We use this rather than Gradle's {@link TestDescriptor} as we want
     * to avoid any nasty memory leak issues that come from keeping Gradle implementation types in memory. Since we
     * use this a the key for our HashMap, it's best to control the implementation as there's no guarantee that Gradle's
     * various {@link TestDescriptor} implementations reliably implement equals and hashCode.
     */
    private static class Descriptor {
        private Descriptor(String name, String className, String parent) {
            this.name = name;
            this.className = className;
            this.parent = parent;
        }

        public static Descriptor of(TestDescriptor d) {
            return new Descriptor(d.getName(), d.getClassName(), d.getParent() == null ? null : d.getParent().toString());
        }

        public boolean equals(Object o) {
            if (DefaultGroovyMethods.is(this, o)) return true;
            if (!getClass().equals(o.getClass())) return false;

            Descriptor that = (Descriptor) o;

            if (!className.equals(that.className)) return false;
            if (!name.equals(that.name)) return false;
            if (!parent.equals(that.parent)) return false;

            return true;
        }

        public int hashCode() {
            int result;
            result = (name != null ? name.hashCode() : 0);
            result = 31 * result + (className != null ? className.hashCode() : 0);
            result = 31 * result + (parent != null ? parent.hashCode() : 0);
            return result;
        }

        private final String name;
        private final String className;
        private final String parent;
    }
}
