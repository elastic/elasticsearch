/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.iplocation;

import org.elasticsearch.action.bulk.FailureStoreMetrics;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.iplocation.api.DatabaseAvailabilityListener;
import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpDataLookupInfo;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.iplocation.api.IpLocationInfoCollector;
import org.elasticsearch.iplocation.api.IpLocationService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.useragent.api.UserAgentParserRegistry;
import org.elasticsearch.xcontent.XContentType;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for a lost-wakeup race between ingest pipeline publication and the one-shot
 * "database became available" notification used to recover IP-location pipelines.
 *
 * <p>The production interleaving this reproduces:
 * <ol>
 *   <li>A cluster-state applier thread builds a pipeline inside
 *       {@link IngestService#innerUpdatePipelines}. Because the database is not yet available, the
 *       {@link GeoIpProcessor.Factory} installs a {@link DatabaseUnavailableProcessor} placeholder.
 *       Crucially, the new pipeline has <em>not</em> yet been published to the {@code pipelines} map.</li>
 *   <li>The database finishes loading and fires the single, edge-triggered
 *       {@link DatabaseAvailabilityListener#onDatabaseAvailable} notification. The listener scans the
 *       {@code pipelines} map for pipelines that still hold a placeholder for that database, and reloads them.</li>
 * </ol>
 *
 * <p>If the listener can observe the {@code pipelines} map while it is still pre-publication, it finds
 * nothing to reload, the one-shot notification is consumed, and the pipeline is then published with the
 * placeholder still in place &mdash; permanently. Documents ingested through it are never enriched and are
 * instead tagged {@code _ip_location_database_unavailable_*}.
 *
 * <p>The fix makes the listener's scan observe a consistent, published view of the pipelines, so it blocks
 * until publication completes and then reloads the now-recoverable pipeline into a live {@link GeoIpProcessor}.
 *
 * <p>The race window is made deterministic with two latches: the test double parks the factory after it has
 * decided the database is unavailable but before publication, then drives the availability notification from a
 * second thread before releasing the factory.
 */
public class IngestIpLocationDatabaseAvailabilityRaceTests extends ESTestCase {

    private static final String DATABASE_FILE = "asn.mmdb";
    private static final String PIPELINE_ID = "ip_location_pipeline";
    private static final String TARGET_FIELD = "out";
    private static final String ENRICHED_FIELD = "organization_name";

    public void testPipelineRecoversWhenDatabaseBecomesAvailableWhilePipelineIsBeingPublished() throws Exception {
        ProjectId projectId = randomProjectIdOrDefault();

        CountDownLatch factoryParked = new CountDownLatch(1);
        CountDownLatch releaseFactory = new CountDownLatch(1);
        LatchedIpLocationService ipLocationService = new LatchedIpLocationService(factoryParked, releaseFactory);

        IngestService ingestService = newIngestService(ipLocationService);
        assertNotNull("plugin must register an availability listener while building processors", ipLocationService.listener);

        ClusterState stateWithPipeline = clusterStateWithIpLocationPipeline(projectId);
        ClusterState previousState = ClusterState.builder(new ClusterName("test"))
            .putProjectMetadata(ProjectMetadata.builder(projectId))
            .build();

        // Thread A: applies the cluster state, building (but not yet publishing) the pipeline. It parks inside the
        // factory while the database is still unavailable.
        AtomicReference<Throwable> applyError = new AtomicReference<>();
        Thread applier = new Thread(() -> {
            try {
                ingestService.applyClusterState(new ClusterChangedEvent("test", stateWithPipeline, previousState));
            } catch (Throwable t) {
                applyError.set(t);
            }
        }, "cluster-state-applier");
        applier.start();

        assertTrue("pipeline build did not reach the factory", factoryParked.await(10, TimeUnit.SECONDS));

        // The database becomes available and the one-shot notification fires while the pipeline is still unpublished.
        ipLocationService.databaseAvailable.set(true);
        AtomicReference<Throwable> notifyError = new AtomicReference<>();

        // Thread B: triggers the DatabaseAvailabilityListener. Normally, this is triggered by the generic thread pool through the
        // DatabaseNodeService once the database becomes available.
        Thread notifier = new Thread(() -> {
            try {
                // the ingest plugin listener now scans the pipelines, looking for any that still hold a DatabaseUnavailableProcessor
                // to replace with a GeoIpProcessor.
                ipLocationService.listener.onDatabaseAvailable(projectId.id(), DATABASE_FILE);
            } catch (Throwable t) {
                notifyError.set(t);
            }
        }, "database-availability-notifier");
        notifier.start();

        // On unfixed code the notifier reads the stale, pre-publication map and returns immediately; on fixed code it
        // blocks until the applier publishes the pipeline. Waiting with a timeout lets us release the factory in both
        // cases while guaranteeing that, on unfixed code, the (now-completed) stale scan happened before publication.
        notifier.join(TimeUnit.SECONDS.toMillis(2));

        // Now release the factory, which will cause the pipeline to be published. On non-synchronized code, this is already too late.
        releaseFactory.countDown();

        applier.join(TimeUnit.SECONDS.toMillis(10));
        notifier.join(TimeUnit.SECONDS.toMillis(10));

        assertNull("applier thread failed", applyError.get());
        assertNull("notifier thread failed", notifyError.get());
        assertFalse("applier thread did not finish", applier.isAlive());
        assertFalse("notifier thread did not finish", notifier.isAlive());

        // The database was available before the notification fired, so the pipeline must have been reconciled to a live
        // processor rather than being left tagging documents forever.
        assertThat(
            "pipeline is stuck with a DatabaseUnavailableProcessor despite the database being available",
            ingestService.getProcessorsInPipeline(projectId, PIPELINE_ID, DatabaseUnavailableProcessor.class),
            empty()
        );
        assertThat(
            "pipeline did not recover into a live GeoIpProcessor",
            ingestService.getProcessorsInPipeline(projectId, PIPELINE_ID, GeoIpProcessor.class),
            hasSize(1)
        );

        // End-to-end symptom check: a document ingested through the recovered pipeline is enriched and not tagged.
        IngestDocument document = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>(Map.of("ip", "89.160.20.128")));
        AtomicReference<Exception> executeError = new AtomicReference<>();
        ingestService.getPipeline(projectId, PIPELINE_ID).execute(document, (ignored, e) -> executeError.set(e));

        assertNull("pipeline execution failed", executeError.get());
        Object outputFieldRaw = document.getSourceAndMetadata().get(TARGET_FIELD);
        assertNotNull("document was not enriched (the reported symptom)", outputFieldRaw);
        @SuppressWarnings("unchecked")
        Map<String, Object> outputField = (Map<String, Object>) outputFieldRaw;
        assertEquals("Elastic", outputField.get(ENRICHED_FIELD));
        List<?> tags = document.getFieldValue("tags", List.class, true);
        assertTrue(
            "document was tagged database-unavailable",
            tags == null || tags.contains("_ip_location_database_unavailable_" + DATABASE_FILE) == false
        );
    }

    private static IngestService newIngestService(IpLocationService ipLocationService) {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        return new IngestService(
            mock(ClusterService.class),
            threadPool,
            null,
            null,
            null,
            List.of(new IngestIpLocationPlugin()),
            mock(Client.class),
            null,
            UserAgentParserRegistry.NOOP,
            ipLocationService,
            FailureStoreMetrics.NOOP,
            TestProjectResolvers.alwaysThrow(),
            new FeatureService(List.of()) {
                @Override
                public boolean clusterHasFeature(ClusterState state, NodeFeature feature) {
                    return false;
                }
            }
        );
    }

    private static ClusterState clusterStateWithIpLocationPipeline(ProjectId projectId) {
        String pipelineJson = Strings.format("""
            {"processors":[{"ip_location":{"field":"ip","target_field":"%s","database_file":"%s"}}]}""", TARGET_FIELD, DATABASE_FILE);
        IngestMetadata ingestMetadata = new IngestMetadata(
            Map.of(PIPELINE_ID, new PipelineConfiguration(PIPELINE_ID, new BytesArray(pipelineJson), XContentType.JSON))
        );
        return ClusterState.builder(new ClusterName("test"))
            .putProjectMetadata(ProjectMetadata.builder(projectId).putCustom(IngestMetadata.TYPE, ingestMetadata))
            .build();
    }

    /**
     * Test double for {@link IpLocationService} that controls database availability and parks the factory in the exact
     * pre-publication window required to reproduce the lost-wakeup race. While unavailable, {@link #createIpDataLookup}
     * signals that it has been entered and blocks until the test releases it, forcing the factory to install a
     * {@link DatabaseUnavailableProcessor}. Once {@link #databaseAvailable} flips, it returns a live (fake) handle so a
     * reload produces a real {@link GeoIpProcessor}.
     */
    @SuppressWarnings("NewClassNamingConvention")
    private static final class LatchedIpLocationService implements IpLocationService {

        private final AtomicBoolean databaseAvailable = new AtomicBoolean(false);
        private final CountDownLatch factoryParked;
        private final CountDownLatch releaseFactory;
        private volatile DatabaseAvailabilityListener listener;

        LatchedIpLocationService(CountDownLatch factoryParked, CountDownLatch releaseFactory) {
            this.factoryParked = factoryParked;
            this.releaseFactory = releaseFactory;
        }

        @Override
        public IpDataLookup createIpDataLookup(String projectId, String databaseFile, List<String> propertyNames) {
            if (databaseAvailable.get() == false) {
                // Reproduce the window: the factory has decided the database is unavailable, but the resulting pipeline
                // has not been published yet. Hold here until the test drives the availability notification.
                factoryParked.countDown();
                try {
                    assertTrue("factory was never released", releaseFactory.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                return null;
            }
            return new FakeIpDataLookup();
        }

        @Override
        public IpDataLookupInfo getIpDataLookupInfo(String databaseFile) {
            return FakeIpDataLookup.INFO;
        }

        @Override
        public void addDatabaseAvailabilityListener(DatabaseAvailabilityListener listener) {
            this.listener = listener;
        }

        @Override
        public void requestDownloads(String projectId, IpLocationConsumer consumer) {}

        @Override
        public void cancelDownloadRequest(String projectId, IpLocationConsumer consumer) {}
    }

    /**
     * Minimal live lookup handle: enrichment is fixed and the metadata advertises a non-null database type so the
     * factory accepts it as an {@code ip_location} processor.
     */
    @SuppressWarnings("NewClassNamingConvention")
    private static final class FakeIpDataLookup implements IpDataLookup {

        private static final IpDataLookupInfo INFO = new IpDataLookupInfo() {
            @Override
            public SequencedMap<String, Class<?>> getFields() {
                SequencedMap<String, Class<?>> fields = new LinkedHashMap<>();
                fields.put(ENRICHED_FIELD, String.class);
                return fields;
            }

            @Override
            public SequencedMap<String, Class<?>> getDefaultFields() {
                return getFields();
            }

            @Override
            public String getDatabaseType() {
                return "asn";
            }
        };

        @Override
        public Boolean lookup(String ip, IpLocationInfoCollector collector) {
            // Unused: the Map-returning lookup below is overridden directly, which is the variant GeoIpProcessor calls.
            return Boolean.TRUE;
        }

        @Override
        public Map<String, Object> lookup(String ip) {
            return Map.of(ENRICHED_FIELD, "Elastic");
        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public IpDataLookupInfo getInfo() {
            return INFO;
        }
    }
}
