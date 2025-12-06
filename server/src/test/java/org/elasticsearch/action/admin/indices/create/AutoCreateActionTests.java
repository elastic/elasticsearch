/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate.DataStreamTemplate;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AutoCreateActionTests extends ESTestCase {

    public void testResolveTemplates() {
        ProjectMetadata projectMetadata;
        {
            ProjectMetadata.Builder mdBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
            DataStreamTemplate dataStreamTemplate = new DataStreamTemplate();
            mdBuilder.put("1", ComposableIndexTemplate.builder().indexPatterns(List.of("legacy-logs-*")).priority(10L).build());
            mdBuilder.put(
                "2",
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of("logs-*"))
                    .priority(20L)
                    .dataStreamTemplate(dataStreamTemplate)
                    .build()
            );
            mdBuilder.put(
                "3",
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of("logs-*"))
                    .priority(30L)
                    .dataStreamTemplate(dataStreamTemplate)
                    .build()
            );
            projectMetadata = mdBuilder.build();
        }

        CreateIndexRequest request = new CreateIndexRequest("logs-foobar");
        ComposableIndexTemplate result = AutoCreateAction.resolveTemplate(request, projectMetadata);
        assertThat(result, notNullValue());
        assertThat(result.getDataStreamTemplate(), notNullValue());

        request = new CreateIndexRequest("logs-barbaz");
        result = AutoCreateAction.resolveTemplate(request, projectMetadata);
        assertThat(result, notNullValue());
        assertThat(result.getDataStreamTemplate(), notNullValue());

        // An index that matches with a template without a data steam definition
        request = new CreateIndexRequest("legacy-logs-foobaz");
        result = AutoCreateAction.resolveTemplate(request, projectMetadata);
        assertThat(result, notNullValue());
        assertThat(result.getDataStreamTemplate(), nullValue());

        // An index that doesn't match with an index template
        request = new CreateIndexRequest("my-index");
        result = AutoCreateAction.resolveTemplate(request, projectMetadata);
        assertThat(result, nullValue());
    }

}
