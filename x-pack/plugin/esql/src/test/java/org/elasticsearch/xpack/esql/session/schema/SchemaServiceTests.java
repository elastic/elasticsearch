/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Exercises the unified-dispatch core of {@link SchemaService} — how a name is classified by its index-abstraction
 * kind, routed to the provider that {@link AbstractionSchemaProvider#handles} it, and the per-provider results are
 * flattened — using lightweight recording providers so the routing logic is tested without standing up the real
 * resolvers. Index-abstraction classification (and so the dataset/view split) is left to integration coverage; here
 * we pin the pure routing, the absent-name fallback, the empty short-circuit, and the no-handler failure.
 */
public class SchemaServiceTests extends ESTestCase {

    /** A provider that handles a fixed set of kinds and records every batch of names it is asked to resolve. */
    private static final class RecordingProvider implements AbstractionSchemaProvider {
        private final EnumSet<IndexAbstraction.Type> handled;
        private final List<List<String>> batches = new ArrayList<>();

        RecordingProvider(EnumSet<IndexAbstraction.Type> handled) {
            this.handled = handled;
        }

        @Override
        public EnumSet<IndexAbstraction.Type> handles() {
            return handled;
        }

        @Override
        public void resolveSchema(
            SchemaContext ctx,
            ProjectMetadata projectMetadata,
            List<String> names,
            ActionListener<List<ResolvedSchema>> listener
        ) {
            batches.add(List.copyOf(names));
            listener.onResponse(
                names.stream()
                    .map(
                        n -> (ResolvedSchema) new ResolvedSchema.Index(
                            n,
                            new Versioned<>(IndexResolution.notFound(n), TransportVersion.current())
                        )
                    )
                    .toList()
            );
        }
    }

    public void testProviderForRoutesEachTypeToTheProviderThatHandlesIt() {
        RecordingProvider indexProvider = new RecordingProvider(
            EnumSet.of(IndexAbstraction.Type.CONCRETE_INDEX, IndexAbstraction.Type.ALIAS, IndexAbstraction.Type.DATA_STREAM)
        );
        RecordingProvider viewProvider = new RecordingProvider(EnumSet.of(IndexAbstraction.Type.VIEW));
        SchemaService service = new SchemaService(List.of(indexProvider, viewProvider));

        assertThat(service.providerFor(IndexAbstraction.Type.CONCRETE_INDEX), sameInstance(indexProvider));
        assertThat(service.providerFor(IndexAbstraction.Type.ALIAS), sameInstance(indexProvider));
        assertThat(service.providerFor(IndexAbstraction.Type.DATA_STREAM), sameInstance(indexProvider));
        assertThat(service.providerFor(IndexAbstraction.Type.VIEW), sameInstance(viewProvider));
    }

    public void testProviderForThrowsWhenNoProviderHandlesTheType() {
        SchemaService service = new SchemaService(List.of(new RecordingProvider(EnumSet.of(IndexAbstraction.Type.VIEW))));
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> service.providerFor(IndexAbstraction.Type.CONCRETE_INDEX)
        );
        assertThat(ex.getMessage(), containsString("no schema provider handles index abstraction type [CONCRETE_INDEX]"));
    }

    public void testResolveSchemaGroupsConcreteAndAbsentNamesAndFlattens() {
        RecordingProvider indexProvider = new RecordingProvider(
            EnumSet.of(IndexAbstraction.Type.CONCRETE_INDEX, IndexAbstraction.Type.ALIAS, IndexAbstraction.Type.DATA_STREAM)
        );
        SchemaService service = new SchemaService(List.of(indexProvider));
        ProjectMetadata project = projectWithIndex("present_index");

        PlainActionFuture<List<ResolvedSchema>> future = new PlainActionFuture<>();
        // "absent_index" is not in the lookup -> classified CONCRETE_INDEX (the fallback) and batched with the present one.
        service.resolveSchema(schemaContext(), project, List.of("present_index", "absent_index"), future);
        List<ResolvedSchema> resolved = future.actionGet();

        assertThat(indexProvider.batches, hasSize(1));
        assertThat(indexProvider.batches.get(0), containsInAnyOrder("present_index", "absent_index"));
        assertThat(resolved.stream().map(ResolvedSchema::name).toList(), containsInAnyOrder("present_index", "absent_index"));
    }

    public void testResolveSchemaShortCircuitsOnNoNames() {
        RecordingProvider indexProvider = new RecordingProvider(EnumSet.of(IndexAbstraction.Type.CONCRETE_INDEX));
        SchemaService service = new SchemaService(List.of(indexProvider));

        PlainActionFuture<List<ResolvedSchema>> future = new PlainActionFuture<>();
        service.resolveSchema(schemaContext(), projectWithIndex("idx"), List.of(), future);

        assertThat(future.actionGet(), hasSize(0));
        assertThat(indexProvider.batches, hasSize(0));
    }

    private static ProjectMetadata projectWithIndex(String name) {
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(
                IndexMetadata.builder(name)
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .build()
                    )
                    .build(),
                false
            )
            .build();
    }

    private static SchemaContext schemaContext() {
        // Dispatch forwards the context untouched and the recording providers ignore it, so a null carrier is fine here.
        return null;
    }
}
