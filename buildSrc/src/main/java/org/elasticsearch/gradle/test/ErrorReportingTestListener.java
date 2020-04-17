package org.elasticsearch.gradle.test;

import org.gradle.api.internal.tasks.testing.logging.FullExceptionFormatter;
import org.gradle.api.internal.tasks.testing.logging.TestExceptionFormatter;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.testing.TestDescriptor;
import org.gradle.api.tasks.testing.TestListener;
import org.gradle.api.tasks.testing.TestOutputEvent;
import org.gradle.api.tasks.testing.TestOutputListener;
import org.gradle.api.tasks.testing.TestResult;
import org.gradle.api.tasks.testing.logging.TestLogging;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ErrorReportingTestListener implements TestOutputListener, TestListener {
    private static final Logger LOGGER = Logging.getLogger(ErrorReportingTestListener.class);
    private static final String REPRODUCE_WITH_PREFIX = "REPRODUCE WITH";

    private final TestExceptionFormatter formatter;
    private final File outputDirectory;
    private Map<Descriptor, EventWriter> eventWriters = new ConcurrentHashMap<>();
    private Map<Descriptor, Deque<String>> reproductionLines = new ConcurrentHashMap<>();
    private Set<Descriptor> failedTests = new LinkedHashSet<>();

    public ErrorReportingTestListener(TestLogging testLogging, File outputDirectory) {
        this.formatter = new FullExceptionFormatter(testLogging);
        this.outputDirectory = outputDirectory;
    }

    @Override
    public void onOutput(TestDescriptor testDescriptor, TestOutputEvent outputEvent) {
        TestDescriptor suite = testDescriptor.getParent();

        // Check if this is output from the test suite itself (e.g. afterTest or beforeTest)
        if (testDescriptor.isComposite()) {
            suite = testDescriptor;
        }

        // Hold on to any repro messages so we can report them immediately on test case failure
        if (outputEvent.getMessage().startsWith(REPRODUCE_WITH_PREFIX)) {
            Deque<String> lines = reproductionLines.computeIfAbsent(Descriptor.of(suite), d -> new LinkedList<>());
            lines.add(outputEvent.getMessage());
        }

        EventWriter eventWriter = eventWriters.computeIfAbsent(Descriptor.of(suite), EventWriter::new);
        eventWriter.write(outputEvent);
    }

    @Override
    public void beforeSuite(TestDescriptor suite) {

    }

    @Override
    public void afterSuite(final TestDescriptor suite, TestResult result) {
        Descriptor descriptor = Descriptor.of(suite);

        try {
            // if the test suite failed, report all captured output
            if (result.getResultType().equals(TestResult.ResultType.FAILURE)) {
                EventWriter eventWriter = eventWriters.get(descriptor);

                if (eventWriter != null) {
                    // It's not explicit what the threading guarantees are for TestListener method execution so we'll
                    // be explicitly safe here to avoid interleaving output from multiple test suites
                    synchronized (this) {
                        // make sure we've flushed everything to disk before reading
                        eventWriter.flush();

                        System.err.println("\n\nSuite: " + suite);

                        try (BufferedReader reader = eventWriter.reader()) {
                            PrintStream out = System.out;
                            for (String message = reader.readLine(); message != null; message = reader.readLine()) {
                                if (message.startsWith("  1> ")) {
                                    out = System.out;
                                } else if (message.startsWith("  2> ")) {
                                    out = System.err;
                                }

                                out.println(message);
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Error reading test suite output", e);
        } finally {
            reproductionLines.remove(descriptor);
            EventWriter writer = eventWriters.remove(descriptor);
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    LOGGER.error("Failed to close test suite output stream", e);
                }
            }
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
                // go back and fetch the reproduction line for this test failure
                Deque<String> lines = reproductionLines.get(Descriptor.of(testDescriptor.getParent()));
                if (lines != null) {
                    String line = lines.getLast();
                    if (line != null) {
                        System.err.print('\n' + line);
                    }
                }

                // include test failure exception stacktraces in test suite output log
                if (result.getExceptions().size() > 0) {
                    String message = formatter.format(testDescriptor, result.getExceptions()).substring(4);
                    EventWriter eventWriter = eventWriters.computeIfAbsent(Descriptor.of(testDescriptor.getParent()), EventWriter::new);

                    eventWriter.write(new TestOutputEvent() {
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

        public String getClassName() {
            return className;
        }

        public String getFullName() {
            return className + "." + name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Descriptor that = (Descriptor) o;
            return Objects.equals(name, that.name) && Objects.equals(className, that.className) && Objects.equals(parent, that.parent);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, className, parent);
        }
    }

    private class EventWriter implements Closeable {
        private final File outputFile;
        private final Writer writer;

        EventWriter(Descriptor descriptor) {
            this.outputFile = new File(outputDirectory, descriptor.getClassName() + ".out");

            FileOutputStream fos;
            try {
                fos = new FileOutputStream(this.outputFile);
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to create test suite output file", e);
            }

            this.writer = new PrintWriter(new BufferedOutputStream(fos));
        }

        public void write(TestOutputEvent event) {
            String prefix;
            if (event.getDestination() == TestOutputEvent.Destination.StdOut) {
                prefix = "  1> ";
            } else {
                prefix = "  2> ";
            }

            try {
                if (event.getMessage().equals("\n")) {
                    writer.write(event.getMessage());
                } else {
                    writer.write(prefix + event.getMessage());
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to write test suite output", e);
            }
        }

        public void flush() throws IOException {
            writer.flush();
        }

        public BufferedReader reader() {
            try {
                return new BufferedReader(new FileReader(outputFile));
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to read test suite output file", e);
            }
        }

        @Override
        public void close() throws IOException {
            writer.close();

            // there's no need to keep this stuff on disk after suite execution
            outputFile.delete();
        }
    }
}
