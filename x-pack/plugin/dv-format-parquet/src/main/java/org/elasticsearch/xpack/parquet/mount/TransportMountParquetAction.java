/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.mount;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.MigrateToDataStreamAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.parquet.docvalues.ParquetBackedSettings;
import org.elasticsearch.xpack.parquet.mount.MountPlanner.IndexPlan;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Mounts Parquet files as the backing indices of a data stream (A1b and A2a of the plan). The steps use the public actions of
 * Elasticsearch and no private entry point:
 *
 * <ol>
 *   <li>a composable index template for the data stream, with the mapping and the columnar settings but with no files, so that a
 *       rollover creates an ordinary columnar index and never attaches the files again;</li>
 *   <li>one index for each index of the plan, with its pieces, one primary shard, and the settings that attach the files;</li>
 *   <li>an alias over the indices, with the last index as its write index;</li>
 *   <li>the migration of the alias to a data stream.</li>
 * </ol>
 *
 * If a step fails, the indices and the template that the mount created are deleted.
 */
public class TransportMountParquetAction extends HandledTransportAction<ParquetMountRequest, ParquetMountResponse> {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(60);

    private final RepositoriesService repositories;
    private final ProjectResolver projectResolver;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public TransportMountParquetAction(
        TransportService transportService,
        ActionFilters actionFilters,
        RepositoriesService repositories,
        ProjectResolver projectResolver,
        ThreadPool threadPool,
        ClusterService clusterService,
        Client client
    ) {
        super(
            MountParquetAction.NAME,
            transportService,
            actionFilters,
            ParquetMountRequest::new,
            threadPool.executor(ThreadPool.Names.GENERIC)
        );
        this.repositories = repositories;
        this.projectResolver = projectResolver;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, ParquetMountRequest request, ActionListener<ParquetMountResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(ActionRunnable.wrap(listener, l -> {
            ParquetMountSpec spec = ParquetMountSpec.parse(request.body(), true);
            requireFreeName(spec.index() != null ? spec.index() : spec.dataStream());
            final ParquetMountAnalysis analysis;
            try {
                analysis = ParquetMountAnalysis.analyze(spec, repositories, projectResolver.getProjectId());
            } catch (NoSuchFileException e) {
                throw new ResourceNotFoundException("the file [" + e.getFile() + "] does not exist");
            }
            if (spec.index() != null) {
                if (analysis.plan().size() != 1) {
                    throw new IllegalArgumentException(
                        "the files need ["
                            + analysis.plan().size()
                            + "] indices with a limit of ["
                            + spec.maxRowsPerIndex()
                            + "] rows for each, and an [index] holds one: use [data_stream], or raise [max_rows_per_index]"
                    );
                }
                mountIndex(analysis, l);
                return;
            }
            if (analysis.mapping().fields().stream().noneMatch(field -> field.name().equals("@timestamp"))) {
                throw new IllegalArgumentException(
                    "a mount creates a data stream, which needs a [@timestamp] field: name the column in [timestamp_field], "
                        + "with a type override of date and the conversion of its values, or have a column named [@timestamp]"
                );
            }
            mount(analysis, l);
        }));
    }

    private void requireFreeName(String name) {
        if (clusterService.state().metadata().getProject(projectResolver.getProjectId()).getIndicesLookup().containsKey(name)) {
            throw new ResourceAlreadyExistsException("an index, an alias or a data stream named [" + name + "] exists already");
        }
    }

    /** Mounts the files as one plain index, which has no template, no alias and no data stream, and so needs no timestamp field. */
    private void mountIndex(ParquetMountAnalysis analysis, ActionListener<ParquetMountResponse> listener) throws IOException {
        String index = analysis.spec().index();
        List<String> created = new ArrayList<>();
        createIndices(analysis, List.of(index), 0, mappingJson(analysis), created, listener.delegateFailureAndWrap((l, ignored) -> {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("acknowledged", true);
            body.put("index", index);
            body.put("row_count", analysis.rows());
            body.put("mapped_fields", analysis.mapping().fields().size());
            body.put("unmapped", analysis.toPreview().get("unmapped"));
            l.onResponse(new ParquetMountResponse(body));
        }));
    }

    private void mount(ParquetMountAnalysis analysis, ActionListener<ParquetMountResponse> listener) throws IOException {
        ParquetMountSpec spec = analysis.spec();
        String dataStream = spec.dataStream();
        String templateName = "parquet-mount-" + dataStream;
        String mappingJson = mappingJson(analysis);
        long now = System.currentTimeMillis();
        List<String> indexNames = new ArrayList<>();
        for (int i = 0; i < analysis.plan().size(); i++) {
            indexNames.add(DataStream.getDefaultBackingIndexName(dataStream, i + 1, now));
        }
        List<String> created = new ArrayList<>();
        ActionListener<ParquetMountResponse> withCleanup = ActionListener.wrap(
            listener::onResponse,
            e -> cleanup(templateName, created, e, listener)
        );

        Settings templateSettings = Settings.builder()
            .put(spec.indexSettings())
            .put("index.mode", "columnar")
            .put("index.number_of_shards", 1)
            .build();
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream))
            .template(new Template(templateSettings, new CompressedXContent(mappingJson), null))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .priority(500L)
            .build();
        TransportPutComposableIndexTemplateAction.Request putTemplate = new TransportPutComposableIndexTemplateAction.Request(templateName)
            .indexTemplate(template)
            .create(true);
        client.execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplate, withCleanup.delegateFailureAndWrap((l, ack) -> {
            created.add(templateName); // the template is cleaned up like an index: see cleanup
            createIndices(analysis, indexNames, 0, mappingJson, created, l.delegateFailureAndWrap((l2, ignored) -> {
                IndicesAliasesRequest aliases = new IndicesAliasesRequest(TIMEOUT, TIMEOUT);
                for (int i = 0; i < indexNames.size(); i++) {
                    aliases.addAliasAction(
                        IndicesAliasesRequest.AliasActions.add()
                            .index(indexNames.get(i))
                            .alias(dataStream)
                            .writeIndex(i == indexNames.size() - 1)
                    );
                }
                client.admin().indices().aliases(aliases, l2.delegateFailureAndWrap((l3, aliasesAck) -> {
                    client.execute(
                        MigrateToDataStreamAction.INSTANCE,
                        new MigrateToDataStreamAction.Request(TIMEOUT, TIMEOUT, dataStream),
                        l3.delegateFailureAndWrap((l4, migrated) -> l4.onResponse(response(analysis, dataStream, templateName, indexNames)))
                    );
                }));
            }));
        }));
    }

    private void createIndices(
        ParquetMountAnalysis analysis,
        List<String> names,
        int next,
        String mappingJson,
        List<String> created,
        ActionListener<Void> listener
    ) {
        if (next == names.size()) {
            listener.onResponse(null);
            return;
        }
        ParquetMountSpec spec = analysis.spec();
        IndexPlan plan = analysis.plan().get(next);
        Settings settings = Settings.builder()
            .put(spec.indexSettings())
            .put("index.mode", "columnar")
            .put(IndexSettings.ID_FIELD_INDEXED.getKey(), false)
            .put("index.number_of_shards", 1)
            .put(ParquetBackedSettings.REPOSITORY.getKey(), spec.repository())
            .putList(ParquetBackedSettings.PIECES.getKey(), analysis.pieces(plan))
            .put(ParquetBackedSettings.FIELDS.getKey(), analysis.mapping().fieldsSetting())
            .build();
        CreateIndexRequest request = new CreateIndexRequest(names.get(next)).settings(settings).mapping(mappingJson);
        client.admin().indices().create(request, listener.delegateFailureAndWrap((l, response) -> {
            created.add(names.get(next));
            createIndices(analysis, names, next + 1, mappingJson, created, l);
        }));
    }

    private static String mappingJson(ParquetMountAnalysis analysis) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject().field("properties", analysis.mapping().properties()).endObject();
            return Strings.toString(builder);
        }
    }

    private static ParquetMountResponse response(ParquetMountAnalysis analysis, String dataStream, String template, List<String> indices) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("acknowledged", true);
        body.put("data_stream", dataStream);
        body.put("template", template);
        body.put("backing_indices", indices);
        body.put("row_count", analysis.rows());
        body.put("mapped_fields", analysis.mapping().fields().size());
        body.put("unmapped", analysis.toPreview().get("unmapped"));
        return new ParquetMountResponse(body);
    }

    /** Deletes what a failed mount created, so that the request can be repeated, and then reports the failure that stopped it. */
    private void cleanup(String templateName, List<String> created, Exception failure, ActionListener<ParquetMountResponse> listener) {
        List<String> indices = created.stream().filter(name -> name.equals(templateName) == false).toList();
        Runnable deleteTemplate = () -> {
            if (created.contains(templateName)) {
                client.execute(
                    TransportDeleteComposableIndexTemplateAction.TYPE,
                    new TransportDeleteComposableIndexTemplateAction.Request(templateName),
                    ActionListener.running(() -> listener.onFailure(failure))
                );
            } else {
                listener.onFailure(failure);
            }
        };
        if (indices.isEmpty()) {
            deleteTemplate.run();
        } else {
            client.admin().indices().delete(new DeleteIndexRequest(indices.toArray(String[]::new)), ActionListener.running(deleteTemplate));
        }
    }
}
