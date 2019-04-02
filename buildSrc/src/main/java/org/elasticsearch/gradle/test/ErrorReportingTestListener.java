package org.elasticsearch.gradle.test;

import org.gradle.api.internal.tasks.testing.logging.FullExceptionFormatter;
import org.gradle.api.internal.tasks.testing.logging.TestExceptionFormatter;
import org.gradle.api.tasks.testing.TestDescriptor;
import org.gradle.api.tasks.testing.TestListener;
import org.gradle.api.tasks.testing.TestOutputEvent;
import org.gradle.api.tasks.testing.TestOutputListener;
import org.gradle.api.tasks.testing.TestResult;
import org.gradle.api.tasks.testing.logging.TestLogging;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ErrorReportingTestListener implements TestOutputListener, TestListener {
    private static final String REPRODUCE_WITH_PREFIX = "REPRODUCE WITH";

    private final TestExceptionFormatter formatter;
    private Map<Descriptor, List<TestOutputEvent>> eventBuffer = new ConcurrentHashMap<>();
    private Set<Descriptor> failedTests = new LinkedHashSet<>();

    public ErrorReportingTestListener(TestLogging testLogging) {
        this.formatter = new FullExceptionFormatter(testLogging);
    }

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
                        System.err.println("\n\nSuite: " + suite);

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
        if (result.getResultType() == TestResult.ResultType.FAILURE) {
            failedTests.add(Descriptor.of(testDescriptor));

            if (testDescriptor.getParent() != null) {
                // go back and find the reproduction line for this test failure
                List<TestOutputEvent> events = eventBuffer.get(Descriptor.of(testDescriptor.getParent()));
                for (int i = events.size() - 1; i >= 0; i--) {
                    String message = events.get(i).getMessage();
                    if (message.startsWith(REPRODUCE_WITH_PREFIX)) {
                        System.err.print('\n' + message);
                        break;
                    }
                }

                // include test failure exception stacktraces in test suite output log
                if (result.getExceptions().size() > 0) {
                    String message = formatter.format(testDescriptor, result.getExceptions()).substring(4);

                    events.add(new TestOutputEvent() {
                        @Override
                        public Destination getDestination() {
                            return Destination.StdErr;
                        }

                        @Override
                        public String getMessage() {
                            return message;
                        }
                    });
                }
            }
        }
    }

    public Set<Descriptor> getFailedTests() {
        return failedTests;
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
    public static class Descriptor {
        private final String name;
        private final String className;
        private final String parent;

        private Descriptor(String name, String className, String parent) {
            this.name = name;
            this.className = className;
            this.parent = parent;
        }

        public static Descriptor of(TestDescriptor d) {
            return new Descriptor(d.getName(), d.getClassName(), d.getParent() == null ? null : d.getParent().toString());
        }

        public String getFullName() {
            return className + "." + name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Descriptor that = (Descriptor) o;
            return Objects.equals(name, that.name) &&
                Objects.equals(className, that.className) &&
                Objects.equals(parent, that.parent);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, className, parent);
        }
    }
}
