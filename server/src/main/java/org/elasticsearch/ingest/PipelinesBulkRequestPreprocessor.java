/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.plugins.internal.DocumentParsingObserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.ingest.IngestService.NOOP_PIPELINE_NAME;

public class PipelinesBulkRequestPreprocessor extends AbstractBulkRequestPreprocessor implements ClusterStateApplier {

    private final IngestService ingestService;

    private volatile ClusterState state;

    public PipelinesBulkRequestPreprocessor(Supplier<DocumentParsingObserver> documentParsingObserver, IngestService ingestService) {
        super(documentParsingObserver);
        this.ingestService = ingestService;
    }

    @Override
    public void applyClusterState(final ClusterChangedEvent event) {
        state = event.state();
    }

    private static final Logger logger = LogManager.getLogger(PipelinesBulkRequestPreprocessor.class);

    @Override
    public boolean needsProcessing(DocWriteRequest docWriteRequest, IndexRequest indexRequest, Metadata metadata) {
        resolvePipelinesAndUpdateIndexRequest(docWriteRequest, indexRequest, metadata);
        return ingestService.hasPipeline(indexRequest);
    }

    @Override
    public boolean hasBeenProcessed(IndexRequest indexRequest) {
        return indexRequest.isPipelineResolved();
    }

    @Override
    public boolean shouldExecuteOnIngestNode() {
        return true;
    }

    @Override
    protected void processIndexRequest(IndexRequest indexRequest, int slot, RefCountingRunnable refs, IntConsumer onDropped,
                                       final BiConsumer<Integer, Exception> onFailure) {
        IngestService.PipelineIterator pipelines = getAndResetPipelines(indexRequest);
        if (pipelines.hasNext() == false) {
            return;
        }

        // start the stopwatch and acquire a ref to indicate that we're working on this document
        final long startTimeInNanos = System.nanoTime();
        ingestMetric.preIngest();
        final Releasable ref = refs.acquire();
        // the document listener gives us three-way logic: a document can fail processing (1), or it can
        // be successfully processed. a successfully processed document can be kept (2) or dropped (3).
        final ActionListener<Boolean> documentListener = ActionListener.runAfter(new ActionListener<>() {
            @Override
            public void onResponse(Boolean kept) {
                assert kept != null;
                if (kept == false) {
                    onDropped.accept(slot);
                }
            }

            @Override
            public void onFailure(Exception e) {
                ingestMetric.ingestFailed();
                onFailure.accept(slot, e);
            }
        }, () -> {
            // regardless of success or failure, we always stop the ingest "stopwatch" and release the ref to indicate
            // that we're finished with this document
            final long ingestTimeInNanos = System.nanoTime() - startTimeInNanos;
            ingestMetric.postIngest(ingestTimeInNanos);
            ref.close();
        });
        DocumentParsingObserver documentParsingObserver = documentParsingObserverSupplier.get();

        IngestDocument ingestDocument = newIngestDocument(indexRequest);

        executePipelines(pipelines, indexRequest, ingestDocument, documentListener);
        indexRequest.setPipelinesHaveRun();

        assert indexRequest.index() != null;
        documentParsingObserver.setIndexName(indexRequest.index());
        documentParsingObserver.close();
    }

    private void executePipelines(
        final IngestService.PipelineIterator pipelines,
        final IndexRequest indexRequest,
        final IngestDocument ingestDocument,
        final ActionListener<Boolean> listener
    ) {
        assert pipelines.hasNext();
        IngestService.PipelineSlot slot = pipelines.next();
        final String pipelineId = slot.id();
        final Pipeline pipeline = slot.pipeline();
        final boolean isFinalPipeline = slot.isFinal();

        // reset the reroute flag, at the start of a new pipeline execution this document hasn't been rerouted yet
        ingestDocument.resetReroute();

        try {
            if (pipeline == null) {
                throw new IllegalArgumentException("pipeline with id [" + pipelineId + "] does not exist");
            }
            indexRequest.addPipeline(pipelineId);
            final String originalIndex = indexRequest.indices()[0];
            executePipeline(ingestDocument, pipeline, (keep, e) -> {
                assert keep != null;

                if (e != null) {
                    logger.debug(
                        () -> format(
                            "failed to execute pipeline [%s] for document [%s/%s]",
                            pipelineId,
                            indexRequest.index(),
                            indexRequest.id()
                        ),
                        e
                    );
                    listener.onFailure(e);
                    return; // document failed!
                }

                if (keep == false) {
                    listener.onResponse(false);
                    return; // document dropped!
                }

                // update the index request so that we can execute additional pipelines (if any), etc
                updateIndexRequestMetadata(indexRequest, ingestDocument.getMetadata());
                try {
                    // check for self-references if necessary, (i.e. if a script processor has run), and clear the bit
                    if (ingestDocument.doNoSelfReferencesCheck()) {
                        CollectionUtils.ensureNoSelfReferences(ingestDocument.getSource(), null);
                        ingestDocument.doNoSelfReferencesCheck(false);
                    }
                } catch (IllegalArgumentException ex) {
                    // An IllegalArgumentException can be thrown when an ingest processor creates a source map that is self-referencing.
                    // In that case, we catch and wrap the exception, so we can include more details
                    listener.onFailure(
                        new IllegalArgumentException(
                            format(
                                "Failed to generate the source document for ingest pipeline [%s] for document [%s/%s]",
                                pipelineId,
                                indexRequest.index(),
                                indexRequest.id()
                            ),
                            ex
                        )
                    );
                    return; // document failed!
                }

                IngestService.PipelineIterator newPipelines = pipelines;
                final String newIndex = indexRequest.indices()[0];

                if (Objects.equals(originalIndex, newIndex) == false) {
                    // final pipelines cannot change the target index (either directly or by way of a reroute)
                    if (isFinalPipeline) {
                        listener.onFailure(
                            new IllegalStateException(
                                format(
                                    "final pipeline [%s] can't change the target index (from [%s] to [%s]) for document [%s]",
                                    pipelineId,
                                    originalIndex,
                                    newIndex,
                                    indexRequest.id()
                                )
                            )
                        );
                        return; // document failed!
                    }

                    // add the index to the document's index history, and check for cycles in the visited indices
                    boolean cycle = ingestDocument.updateIndexHistory(newIndex) == false;
                    if (cycle) {
                        List<String> indexCycle = new ArrayList<>(ingestDocument.getIndexHistory());
                        indexCycle.add(newIndex);
                        listener.onFailure(
                            new IllegalStateException(
                                format(
                                    "index cycle detected while processing pipeline [%s] for document [%s]: %s",
                                    pipelineId,
                                    indexRequest.id(),
                                    indexCycle
                                )
                            )
                        );
                        return; // document failed!
                    }

                    // clear the current pipeline, then re-resolve the pipelines for this request
                    indexRequest.setPipeline(null);
                    indexRequest.isPipelineResolved(false);
                    resolvePipelinesAndUpdateIndexRequest(null, indexRequest, state.metadata());
                    newPipelines = getAndResetPipelines(indexRequest);

                    // for backwards compatibility, when a pipeline changes the target index for a document without using the reroute
                    // mechanism, do not invoke the default pipeline of the new target index
                    if (ingestDocument.isReroute() == false) {
                        newPipelines = newPipelines.withoutDefaultPipeline();
                    }
                }

                if (newPipelines.hasNext()) {
                    executePipelines(newPipelines, indexRequest, ingestDocument, listener);
                } else {
                    // update the index request's source and (potentially) cache the timestamp for TSDB
                    updateIndexRequestSource(indexRequest, ingestDocument);
                    cacheRawTimestamp(indexRequest, ingestDocument);
                    listener.onResponse(true); // document succeeded!
                }
            });
        } catch (Exception e) {
            logger.debug(
                () -> format("failed to execute pipeline [%s] for document [%s/%s]", pipelineId, indexRequest.index(), indexRequest.id()),
                e
            );
            listener.onFailure(e); // document failed!
        }
    }

    private static void executePipeline(
        final IngestDocument ingestDocument,
        final Pipeline pipeline,
        final BiConsumer<Boolean, Exception> handler
    ) {
        // adapt our {@code BiConsumer<Boolean, Exception>} handler shape to the
        // {@code BiConsumer<IngestDocument, Exception>} handler shape used internally
        // by ingest pipelines and processors
        ingestDocument.executePipeline(pipeline, (result, e) -> {
            if (e != null) {
                handler.accept(true, e);
            } else {
                handler.accept(result != null, null);
            }
        });
    }


    /**
     * Updates an index request based on the metadata of an ingest document.
     */
    private static void updateIndexRequestMetadata(final IndexRequest request, final org.elasticsearch.script.Metadata metadata) {
        // it's fine to set all metadata fields all the time, as ingest document holds their starting values
        // before ingestion, which might also get modified during ingestion.
        request.index(metadata.getIndex());
        request.id(metadata.getId());
        request.routing(metadata.getRouting());
        request.version(metadata.getVersion());
        if (metadata.getVersionType() != null) {
            request.versionType(VersionType.fromString(metadata.getVersionType()));
        }
        Number number;
        if ((number = metadata.getIfSeqNo()) != null) {
            request.setIfSeqNo(number.longValue());
        }
        if ((number = metadata.getIfPrimaryTerm()) != null) {
            request.setIfPrimaryTerm(number.longValue());
        }
        Map<String, String> map;
        if ((map = metadata.getDynamicTemplates()) != null) {
            Map<String, String> mergedDynamicTemplates = new HashMap<>(request.getDynamicTemplates());
            mergedDynamicTemplates.putAll(map);
            request.setDynamicTemplates(mergedDynamicTemplates);
        }
    }

    /**
     * Returns the pipelines of the request, and updates the request so that it no longer references
     * any pipelines (both the default and final pipeline are set to the noop pipeline).
     */
    private IngestService.PipelineIterator getAndResetPipelines(IndexRequest indexRequest) {
        final String pipelineId = indexRequest.getPipeline();
        indexRequest.setPipeline(NOOP_PIPELINE_NAME);
        final String finalPipelineId = indexRequest.getFinalPipeline();
        indexRequest.setFinalPipeline(NOOP_PIPELINE_NAME);
        return ingestService.new PipelineIterator(pipelineId, finalPipelineId);
    }


    /**
     * Resolves the potential pipelines (default and final) from the requests or templates associated to the index and then **mutates**
     * the {@link org.elasticsearch.action.index.IndexRequest} passed object with the pipeline information.
     * <p>
     * Also, this method marks the request as `isPipelinesResolved = true`: Due to the request could be rerouted from a coordinating node
     * to an ingest node, we have to be able to avoid double resolving the pipelines and also able to distinguish that either the pipeline
     * comes as part of the request or resolved from this method. All this is made to later be able to reject the request in case the
     * pipeline was set by a required pipeline **and** the request also has a pipeline request too.
     *
     * @param originalRequest Original write request received.
     * @param indexRequest    The {@link org.elasticsearch.action.index.IndexRequest} object to update.
     * @param metadata        Cluster metadata from where the pipeline information could be derived.
     */
    public static void resolvePipelinesAndUpdateIndexRequest(
        final DocWriteRequest<?> originalRequest,
        final IndexRequest indexRequest,
        final Metadata metadata
    ) {
        resolvePipelinesAndUpdateIndexRequest(originalRequest, indexRequest, metadata, System.currentTimeMillis());
    }

    static void resolvePipelinesAndUpdateIndexRequest(
        final DocWriteRequest<?> originalRequest,
        final IndexRequest indexRequest,
        final Metadata metadata,
        final long epochMillis
    ) {
        if (indexRequest.isPipelineResolved()) {
            return;
        }

        String requestPipeline = indexRequest.getPipeline();

        Pipelines pipelines = resolvePipelinesFromMetadata(originalRequest, indexRequest, metadata, epochMillis) //
            .or(() -> resolvePipelinesFromIndexTemplates(indexRequest, metadata))
            .orElse(Pipelines.NO_PIPELINES_DEFINED);

        // The pipeline coming as part of the request always has priority over the resolved one from metadata or templates
        if (requestPipeline != null) {
            indexRequest.setPipeline(requestPipeline);
        } else {
            indexRequest.setPipeline(pipelines.defaultPipeline);
        }
        indexRequest.setFinalPipeline(pipelines.finalPipeline);
        indexRequest.isPipelineResolved(true);
    }


    private static Optional<Pipelines> resolvePipelinesFromMetadata(
        DocWriteRequest<?> originalRequest,
        IndexRequest indexRequest,
        Metadata metadata,
        long epochMillis
    ) {
        IndexMetadata indexMetadata = null;
        // start to look for default or final pipelines via settings found in the cluster metadata
        if (originalRequest != null) {
            indexMetadata = metadata.indices()
                .get(IndexNameExpressionResolver.resolveDateMathExpression(originalRequest.index(), epochMillis));
        }
        // check the alias for the index request (this is how normal index requests are modeled)
        if (indexMetadata == null && indexRequest.index() != null) {
            IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(indexRequest.index());
            if (indexAbstraction != null && indexAbstraction.getWriteIndex() != null) {
                indexMetadata = metadata.index(indexAbstraction.getWriteIndex());
            }
        }
        // check the alias for the action request (this is how upserts are modeled)
        if (indexMetadata == null && originalRequest != null && originalRequest.index() != null) {
            IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(originalRequest.index());
            if (indexAbstraction != null && indexAbstraction.getWriteIndex() != null) {
                indexMetadata = metadata.index(indexAbstraction.getWriteIndex());
            }
        }

        if (indexMetadata == null) {
            return Optional.empty();
        }

        final Settings settings = indexMetadata.getSettings();
        return Optional.of(new Pipelines(IndexSettings.DEFAULT_PIPELINE.get(settings), IndexSettings.FINAL_PIPELINE.get(settings)));
    }

    private static Optional<Pipelines> resolvePipelinesFromIndexTemplates(IndexRequest indexRequest, Metadata metadata) {
        if (indexRequest.index() == null) {
            return Optional.empty();
        }

        // the index does not exist yet (and this is a valid request), so match index
        // templates to look for pipelines in either a matching V2 template (which takes
        // precedence), or if a V2 template does not match, any V1 templates
        String v2Template = MetadataIndexTemplateService.findV2Template(metadata, indexRequest.index(), false);
        if (v2Template != null) {
            final Settings settings = MetadataIndexTemplateService.resolveSettings(metadata, v2Template);
            return Optional.of(new Pipelines(IndexSettings.DEFAULT_PIPELINE.get(settings), IndexSettings.FINAL_PIPELINE.get(settings)));
        }

        String defaultPipeline = null;
        String finalPipeline = null;
        List<IndexTemplateMetadata> templates = MetadataIndexTemplateService.findV1Templates(metadata, indexRequest.index(), null);
        // order of templates are the highest order first
        for (final IndexTemplateMetadata template : templates) {
            final Settings settings = template.settings();

            // note: the exists/get trickiness here is because we explicitly *don't* want the default value
            // of the settings -- a non-null value would terminate the search too soon
            if (defaultPipeline == null && IndexSettings.DEFAULT_PIPELINE.exists(settings)) {
                defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(settings);
                // we can not break in case a lower-order template has a final pipeline that we need to collect
            }
            if (finalPipeline == null && IndexSettings.FINAL_PIPELINE.exists(settings)) {
                finalPipeline = IndexSettings.FINAL_PIPELINE.get(settings);
                // we can not break in case a lower-order template has a default pipeline that we need to collect
            }
            if (defaultPipeline != null && finalPipeline != null) {
                // we can break if we have already collected a default and final pipeline
                break;
            }
        }

        // having exhausted the search, if nothing was found, then use the default noop pipeline names
        defaultPipeline = Objects.requireNonNullElse(defaultPipeline, NOOP_PIPELINE_NAME);
        finalPipeline = Objects.requireNonNullElse(finalPipeline, NOOP_PIPELINE_NAME);

        return Optional.of(new Pipelines(defaultPipeline, finalPipeline));
    }


    /**
     * Grab the @timestamp and store it on the index request so that TSDB can use it without needing to parse
     * the source for this document.
     */
    private static void cacheRawTimestamp(final IndexRequest request, final IngestDocument document) {
        if (request.getRawTimestamp() == null) {
            // cache the @timestamp from the ingest document's source map if there is one
            Object rawTimestamp = document.getSource().get(DataStream.TIMESTAMP_FIELD_NAME);
            if (rawTimestamp != null) {
                request.setRawTimestamp(rawTimestamp);
            }
        }
    }

    record Pipelines(String defaultPipeline, String finalPipeline) {

        static final Pipelines NO_PIPELINES_DEFINED = new Pipelines(NOOP_PIPELINE_NAME, NOOP_PIPELINE_NAME);

        public Pipelines {
            Objects.requireNonNull(defaultPipeline);
            Objects.requireNonNull(finalPipeline);
        }
    }
}
