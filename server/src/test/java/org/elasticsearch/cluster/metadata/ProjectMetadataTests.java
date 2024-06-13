/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.upgrades.FeatureMigrationResults;
import org.elasticsearch.xcontent.ToXContent;

import java.util.function.Function;

public class ProjectMetadataTests extends ESTestCase {

    public static int expectedChunkCount(ToXContent.Params params, ProjectMetadata project) {
        final var context = Metadata.XContentContext.from(params);

        long count = 0;
        if (context == Metadata.XContentContext.API) {
            // 2 chunks wrapping "indices"" and one chunk per index
            count += 2 + project.indices().size();
        }

        // 2 chunks wrapping "templates" and one chunk per template
        count += 2 + project.templates().size();

        for (ProjectMetadata.ProjectCustom custom : project.customs().values()) {
            count += 2;  // open / close object
            if (custom instanceof ComponentTemplateMetadata componentTemplateMetadata) {
                count += 2 + componentTemplateMetadata.componentTemplates().size();
            } else if (custom instanceof ComposableIndexTemplateMetadata composableIndexTemplateMetadata) {
                count += 2 + composableIndexTemplateMetadata.indexTemplates().size();
            } else if (custom instanceof DataStreamMetadata dataStreamMetadata) {
                count += 4 + dataStreamMetadata.dataStreams().size() + dataStreamMetadata.getDataStreamAliases().size();
            } else if (custom instanceof FeatureMigrationResults featureMigrationResults) {
                count += 2 + featureMigrationResults.getFeatureStatuses().size();
            } else if (custom instanceof IndexGraveyard indexGraveyard) {
                count += 2 + indexGraveyard.getTombstones().size();
            } else if (custom instanceof IngestMetadata ingestMetadata) {
                count += 2 + ingestMetadata.getPipelines().size();
            } else if (custom instanceof PersistentTasksCustomMetadata persistentTasksCustomMetadata) {
                count += 3 + persistentTasksCustomMetadata.tasks().size();
            } else if (custom instanceof RepositoriesMetadata repositoriesMetadata) {
                count += repositoriesMetadata.repositories().size();
            } else {
                // could be anything, we have to just try it
                count += Iterables.size(
                    (Iterable<ToXContent>) (() -> Iterators.map(custom.toXContentChunked(params), Function.identity()))
                );
            }
        }
        return Math.toIntExact(count);
    }

}
