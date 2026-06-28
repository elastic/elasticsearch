/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.audit.logfile.CapturingLogger;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A micro-benchmark harness (not a correctness test) for {@link LoggingAuditTrail#accessGranted}, the hottest audit code path. It
 * drives a single, fixed {@code access_granted} event through the full audit pipeline — building the log entry and rendering it to
 * JSON via the real {@code appender.audit_rolling.layout.pattern} — so the measured cost reflects exactly what the transport thread
 * pays per audited request.
 *
 * <p>The body is skipped unless {@code -Dtests.audit.profile=true} is set, so it never runs (or slows down) CI. Inputs are fully
 * deterministic so two runs are directly comparable, and the harness only touches stable public API of {@link LoggingAuditTrail}, so
 * it compiles and runs unchanged against both the optimized and the pre-optimization implementations.
 *
 * <p><b>Producing before/after numbers (commit, then revert):</b>
 * <pre>
 *   # 0. JAVA_HOME must point at a JDK 25 (e.g. a Gradle-provisioned one under ~/.gradle/jdks)
 *   # 1. Commit the profiling test on its own so it survives the revert below:
 *   git add x-pack/plugin/security/src/test/java/org/elasticsearch/xpack/security/audit/logfile/LoggingAuditTrailProfilingTests.java
 *   git commit -m "Add audit access_granted profiling harness"
 *   # 2. Commit the optimization (the new accumulator + LoggingAuditTrail + log4j2.properties):
 *   git add -A &amp;&amp; git commit -m "Speed up audit log entry construction"
 *   # 3. Profile the NEW code:
 *   ./gradlew :x-pack:plugin:security:test --tests "*LoggingAuditTrailProfilingTests" \
 *       -Dtests.audit.profile=true -Dtests.output=always
 *   # 4. Revert ONLY the optimization commit (the profiling commit from step 1 stays in place):
 *   git revert --no-edit HEAD
 *   # 5. Profile the OLD code (same command as step 3), then compare the two "avg ns/op" lines.
 *   # 6. Restore the optimization when done:
 *   git revert --no-edit HEAD
 * </pre>
 *
 * <p>Iteration counts can be tuned with {@code -Dtests.audit.profile.warmup} and {@code -Dtests.audit.profile.iterations}.
 */
public class LoggingAuditTrailProfilingTests extends ESTestCase {

    private static final Logger logger = LogManager.getLogger(LoggingAuditTrailProfilingTests.class);

    // The fields are cleared from the in-memory capturing appender in batches of this size, so memory stays bounded across millions of
    // iterations. Clearing happens outside the timed section.
    private static final int BATCH_SIZE = 50_000;

    private static PatternLayout patternLayout;

    @BeforeClass
    public static void lookupPatternLayout() throws Exception {
        // Build the layout from the real audit pattern on the classpath, exactly as LoggingAuditTrailTests does. This is what makes the
        // before/after comparison meaningful: the OLD checkout renders via %varsNotEmpty{%map{...}}, the NEW one via %m -> formatTo.
        final Properties properties = new Properties();
        try (InputStream configStream = LoggingAuditTrail.class.getClassLoader().getResourceAsStream("log4j2.properties")) {
            properties.load(configStream);
        }
        final String patternLayoutFormat = properties.getProperty("appender.audit_rolling.layout.pattern");
        patternLayout = PatternLayout.newBuilder().withPattern(patternLayoutFormat).withCharset(StandardCharsets.UTF_8).build();
    }

    @AfterClass
    public static void releasePatternLayout() {
        patternLayout = null;
    }

    public void testProfileAccessGranted() throws Exception {
        assumeTrue("audit profiling harness; enable with -Dtests.audit.profile=true", Boolean.getBoolean("tests.audit.profile"));
        final int warmupIterations = Integer.getInteger("tests.audit.profile.warmup", 200_000);
        final int measureIterations = Integer.getInteger("tests.audit.profile.iterations", 2_000_000);

        final Settings settings = Settings.builder()
            // emit every common field so the entry is representative of the heaviest realistic case
            .put(LoggingAuditTrail.EMIT_HOST_ADDRESS_SETTING.getKey(), true)
            .put(LoggingAuditTrail.EMIT_HOST_NAME_SETTING.getKey(), true)
            .put(LoggingAuditTrail.EMIT_NODE_NAME_SETTING.getKey(), true)
            .put(LoggingAuditTrail.EMIT_NODE_ID_SETTING.getKey(), true)
            .put(LoggingAuditTrail.EMIT_CLUSTER_NAME_SETTING.getKey(), true)
            .put(LoggingAuditTrail.EMIT_CLUSTER_UUID_SETTING.getKey(), true)
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "profiling-cluster")
            .put(Node.NODE_NAME_SETTING.getKey(), "profiling-node")
            .putList(LoggingAuditTrail.INCLUDE_EVENT_SETTINGS.getKey(), "_all")
            .put(LoggingAuditTrail.INCLUDE_REQUEST_BODY.getKey(), true)
            .build();

        final DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(localNode.getId()).thenReturn("profiling-node-id");
        when(localNode.getAddress()).thenReturn(buildNewFakeTransportAddress());
        final ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings))
            .metadata(Metadata.builder().clusterUUID("profiling-cluster-uuid").build())
            .build();
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(localNode);
        when(clusterService.getClusterName()).thenReturn(ClusterName.CLUSTER_NAME_SETTING.get(settings));
        when(clusterService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
        when(clusterService.state()).thenReturn(clusterState);
        doAnswer(invocation -> {
            ((LoggingAuditTrail) invocation.getArguments()[0]).updateLocalNodeInfo(localNode);
            return null;
        }).when(clusterService).addListener(isA(LoggingAuditTrail.class));
        final ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(
                LoggingAuditTrail.EMIT_HOST_ADDRESS_SETTING,
                LoggingAuditTrail.EMIT_HOST_NAME_SETTING,
                LoggingAuditTrail.EMIT_NODE_NAME_SETTING,
                LoggingAuditTrail.EMIT_NODE_ID_SETTING,
                LoggingAuditTrail.EMIT_CLUSTER_NAME_SETTING,
                LoggingAuditTrail.EMIT_CLUSTER_UUID_SETTING,
                LoggingAuditTrail.INCLUDE_EVENT_SETTINGS,
                LoggingAuditTrail.EXCLUDE_EVENT_SETTINGS,
                LoggingAuditTrail.INCLUDE_REQUEST_BODY,
                LoggingAuditTrail.FILTER_POLICY_IGNORE_PRINCIPALS,
                LoggingAuditTrail.FILTER_POLICY_IGNORE_REALMS,
                LoggingAuditTrail.FILTER_POLICY_IGNORE_ROLES,
                LoggingAuditTrail.FILTER_POLICY_IGNORE_INDICES,
                LoggingAuditTrail.FILTER_POLICY_IGNORE_ACTIONS,
                Loggers.LOG_LEVEL_SETTING
            )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(Task.X_OPAQUE_ID_HTTP_HEADER, "profiling-opaque-id");
        threadContext.putHeader(Task.TRACE_ID, "00000000000000000000000000000001");
        threadContext.putHeader(AuditTrail.X_FORWARDED_FOR_HEADER, "203.0.113.195, 70.41.3.18, 150.172.238.178");

        // INFO level so the audit event is actually emitted (and therefore rendered) on every call
        final Logger capturingLogger = CapturingLogger.newCapturingLogger(Level.INFO, patternLayout);
        // Stop audit events from propagating to the root/console appenders the test framework configures. Otherwise we would be timing
        // (and flooding the output with) console I/O instead of the entry construction + JSON rendering we want to measure. Only the
        // in-memory capturing appender, which renders each entry via the audit layout, should consume the events.
        final LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        final LoggerConfig loggerConfig = loggerContext.getConfiguration().getLoggerConfig(capturingLogger.getName());
        assertThat("expected a dedicated logger config for the capturing logger", loggerConfig.getName(), equalTo(capturingLogger.getName()));
        loggerConfig.setAdditive(false);
        loggerContext.updateLoggers();
        final LoggingAuditTrail auditTrail = new LoggingAuditTrail(settings, clusterService, capturingLogger, threadContext);
        final List<String> output = CapturingLogger.output(capturingLogger.getName(), Level.INFO);

        // Fixed, representative inputs for a transport access_granted event.
        final String requestId = "profiling-request-id";
        final String action = "indices:data/read/search";
        final User user = new User("profiled_user", "audit_role_1", "audit_role_2", "audit_role_3");
        final Authentication authentication = Authentication.newRealmAuthentication(
            user,
            new Authentication.RealmRef("file", "file", "profiling-node")
        );
        final String[] roles = user.roles();
        final AuthorizationInfo authorizationInfo = () -> Map.of(LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME, roles);
        final TransportRequest request = new ProfilingIndicesRequest(
            new String[] { "index-1", "index-2", "index-3" },
            new InetSocketAddress(InetAddress.getLoopbackAddress(), 9300)
        );

        // Sanity check: the configured event must actually be emitted, otherwise we would be timing a no-op.
        auditTrail.accessGranted(requestId, authentication, action, request, authorizationInfo);
        assertThat("accessGranted should emit exactly one audit entry", output.size(), equalTo(1));
        output.clear();

        runBatches(auditTrail, requestId, authentication, action, request, authorizationInfo, output, warmupIterations);
        final long elapsedNanos = runBatches(
            auditTrail,
            requestId,
            authentication,
            action,
            request,
            authorizationInfo,
            output,
            measureIterations
        );

        final double nsPerOp = (double) elapsedNanos / measureIterations;
        final double opsPerSec = 1_000_000_000.0d * measureIterations / elapsedNanos;
        logger.info("==== LoggingAuditTrail#accessGranted profiling ====");
        logger.info("warmup iterations : {}", warmupIterations);
        logger.info("measured iterations: {}", measureIterations);
        logger.info("total elapsed      : {} ms", TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
        logger.info("avg                : {} ns/op", String.format(Locale.ROOT, "%.1f", nsPerOp));
        logger.info("throughput         : {} ops/sec", String.format(Locale.ROOT, "%.0f", opsPerSec));
        logger.info("===================================================");

        // Guard against the loop being optimized away entirely.
        assertThat(elapsedNanos, greaterThan(0L));
    }

    /**
     * Invokes {@code accessGranted} {@code totalIterations} times and returns the time spent strictly inside the invocation loop. The
     * captured audit output is drained between batches (outside the timed section) so memory stays bounded without polluting the
     * measurement.
     */
    private static long runBatches(
        LoggingAuditTrail auditTrail,
        String requestId,
        Authentication authentication,
        String action,
        TransportRequest request,
        AuthorizationInfo authorizationInfo,
        List<String> output,
        int totalIterations
    ) {
        long elapsedNanos = 0L;
        int done = 0;
        while (done < totalIterations) {
            final int batch = Math.min(BATCH_SIZE, totalIterations - done);
            final long start = System.nanoTime();
            for (int i = 0; i < batch; i++) {
                auditTrail.accessGranted(requestId, authentication, action, request, authorizationInfo);
            }
            elapsedNanos += System.nanoTime() - start;
            // drain the rendered entries kept by the capturing appender; not part of the measured time
            output.clear();
            done += batch;
        }
        return elapsedNanos;
    }

    /**
     * Minimal transport request carrying a fixed set of indices and a transport remote address, so the audited entry exercises the
     * {@code indices} array field and the transport origin fields.
     */
    private static final class ProfilingIndicesRequest extends AbstractTransportRequest implements IndicesRequest {

        private final String[] indices;

        private ProfilingIndicesRequest(String[] indices, InetSocketAddress remoteAddress) {
            this.indices = indices;
            remoteAddress(remoteAddress);
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictExpandOpenAndForbidClosed();
        }

        @Override
        public void writeTo(StreamOutput out) {}
    }
}
