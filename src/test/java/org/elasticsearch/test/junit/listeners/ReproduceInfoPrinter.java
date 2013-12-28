package org.elasticsearch.test.junit.listeners;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.ReproduceErrorMessageBuilder;
import com.carrotsearch.randomizedtesting.SeedUtils;
import com.carrotsearch.randomizedtesting.TraceFormatting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.TestCluster;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_ITERATIONS;
import static org.elasticsearch.test.TestCluster.SHARED_CLUSTER_SEED;
import static org.elasticsearch.test.TestCluster.TESTS_CLUSTER_SEED;

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
        ReproduceErrorMessageBuilder builder = reproduceErrorMessageBuilder(b).appendAllOpts(failure.getDescription());

        if (mustAppendClusterSeed(failure)) {
            appendClusterSeed(builder);
        }

        b.append("\n");
        b.append("Throwable:\n");
        if (failure.getException() != null) {
            traces().formatThrowable(b, failure.getException());
        }

        logger.error(b.toString());
    }

    protected boolean mustAppendClusterSeed(Failure failure) {
        return ElasticsearchIntegrationTest.class.isAssignableFrom(failure.getDescription().getTestClass());
    }

    protected void appendClusterSeed(ReproduceErrorMessageBuilder builder) {
        builder.appendOpt(TESTS_CLUSTER_SEED, SeedUtils.formatSeed(SHARED_CLUSTER_SEED));
    }

    protected ReproduceErrorMessageBuilder reproduceErrorMessageBuilder(StringBuilder b) {
        return new MavenMessageBuilder(b);
    }

    protected TraceFormatting traces() {
        TraceFormatting traces = new TraceFormatting();
        try {
            traces = RandomizedContext.current().getRunner().getTraceFormatting();
        } catch (IllegalStateException e) {
            // Ignore if no context.
        }
        return traces;
    }

    protected static class MavenMessageBuilder extends ReproduceErrorMessageBuilder {

        public MavenMessageBuilder(StringBuilder b) {
            super(b);
        }

        @Override
        public ReproduceErrorMessageBuilder appendAllOpts(Description description) {
            super.appendAllOpts(description);
            return appendESProperties();
        }

        /**
         * Append a single VM option.
         */
        @Override
        public ReproduceErrorMessageBuilder appendOpt(String sysPropName, String value) {
            if (sysPropName.equals(SYSPROP_ITERATIONS())) { // we don't want the iters to be in there!
                return this;
            }
            if (Strings.hasLength(value)) {
                return super.appendOpt(sysPropName, value);
            } 
            return this;
        }

        public ReproduceErrorMessageBuilder appendESProperties() {
            appendProperties("es.logger.level", "es.node.mode", "es.node.local", TestCluster.TESTS_ENABLE_MOCK_MODULES);

            if (System.getProperty("tests.jvm.argline") != null && !System.getProperty("tests.jvm.argline").isEmpty()) {
                appendOpt("tests.jvm.argline", "\"" + System.getProperty("tests.jvm.argline") + "\"");
            }
            return this;
        }

        protected ReproduceErrorMessageBuilder appendProperties(String... properties) {
            for (String sysPropName : properties) {
                if (Strings.hasLength(System.getProperty(sysPropName))) {
                    appendOpt(sysPropName, System.getProperty(sysPropName));
                }
            }
            return this;
        }

    }
}
