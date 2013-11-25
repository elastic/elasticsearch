package org.elasticsearch.test.junit.listeners;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.ReproduceErrorMessageBuilder;
import com.carrotsearch.randomizedtesting.SeedUtils;
import com.carrotsearch.randomizedtesting.TraceFormatting;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A {@link RunListener} that emits to {@link System#err} a string with command
 * line parameters allowing quick test re-run under MVN command line.
 */
public class ReproduceInfoPrinter extends RunListener {

    protected final ESLogger logger = Loggers.getLogger(ElasticsearchTestCase.class);

    @Override
    public void testStarted(Description description) throws Exception {
        logger.info("Test {} started", description.getDisplayName());
    }

    @Override
    public void testFinished(Description description) throws Exception {
        logger.info("Test {} finished", description.getDisplayName());
    }

    @Override
    public void testFailure(Failure failure) throws Exception {
        // Ignore assumptions.
        if (failure.getException() instanceof AssumptionViolatedException) {
            return;
        }

        final Description d = failure.getDescription();
        final StringBuilder b = new StringBuilder();
        b.append("FAILURE  : ").append(d.getDisplayName()).append("\n");
        b.append("REPRODUCE WITH  : mvn test");
        ReproduceErrorMessageBuilder builder = new MavenMessageBuilder(b).appendAllOpts(failure.getDescription());
        if (ElasticsearchIntegrationTest.class.isAssignableFrom(failure.getDescription().getTestClass())) {
            builder.appendOpt("tests.cluster_seed", SeedUtils.formatSeed(ElasticsearchIntegrationTest.SHARED_CLUSTER_SEED));
        }

        b.append("\n");
        b.append("Throwable:\n");
        if (failure.getException() != null) {
            TraceFormatting traces = new TraceFormatting();
            try {
                traces = RandomizedContext.current().getRunner().getTraceFormatting();
            } catch (IllegalStateException e) {
                // Ignore if no context.
            }
            traces.formatThrowable(b, failure.getException());
        }
        logger.error(b.toString());
    }

    private static class MavenMessageBuilder extends ReproduceErrorMessageBuilder {

        public MavenMessageBuilder(StringBuilder b) {
            super(b);
        }

        @Override
        public ReproduceErrorMessageBuilder appendAllOpts(Description description) {
            super.appendAllOpts(description);
            appendJVMArgLine();
            return appendESProperties();
        }

        /**
         * Append a single VM option.
         */
        public ReproduceErrorMessageBuilder appendOpt(String sysPropName, String value) {
            if (sysPropName.equals("tests.iters")) { // we don't want the iters to be in there!
                return this;
            }
            if (value != null && !value.isEmpty()) {
                return super.appendOpt(sysPropName, value);
            } 
            return this;
        }

        public ReproduceErrorMessageBuilder appendESProperties() {
            for (String sysPropName : Arrays.asList(
                    "es.logger.level", "es.node.mode", "es.node.local")) {
                if (System.getProperty(sysPropName) != null && !System.getProperty(sysPropName).isEmpty()) {
                    appendOpt(sysPropName, System.getProperty(sysPropName));
                }
            }
            return this;
        }

        public ReproduceErrorMessageBuilder appendJVMArgLine() {
            StringBuilder builder = new StringBuilder();
            Set<String> values = new HashSet<String>();
            for (String sysPropName : Arrays.asList(
                    "tests.jvm.option1", "tests.jvm.option2", "tests.jvm.option3", "tests.jvm.option4", "tests.jvm.argline")) {
                if (System.getProperty(sysPropName) != null && !System.getProperty(sysPropName).isEmpty()) {
                    String propValue = System.getProperty(sysPropName).trim();
                    if (!values.contains(propValue)) {
                        builder.append(propValue);
                        values.add(propValue); // deduplicate
                        builder.append(' ');
                    }
                }
            }
            if (builder.length() > 0) {
                appendOpt("tests.jvm.argline", "\"" + builder.toString().trim() + "\"");
            }

            return this;
        }
    }
}
