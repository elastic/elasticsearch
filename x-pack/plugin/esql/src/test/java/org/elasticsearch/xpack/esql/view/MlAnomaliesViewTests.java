/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.is;

public class MlAnomaliesViewTests extends ESTestCase {

    public void testNoResultsIndex() {
        assertThat(MlAnomaliesView.hasResultsIndex(ProjectMetadata.builder(ProjectId.DEFAULT).build()), is(false));
    }

    public void testUnrelatedIndicesAreIgnored() {
        var project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(index(".ml-annotations-000001"))
            .put(index(".ml-anomalies"))
            .put(index("my-index"))
            .build();
        assertThat(MlAnomaliesView.hasResultsIndex(project), is(false));
    }

    public void testResultsIndex() {
        var project = ProjectMetadata.builder(ProjectId.DEFAULT).put(index(".ml-anomalies-shared")).put(index("my-index")).build();
        assertThat(MlAnomaliesView.hasResultsIndex(project), is(true));
    }

    public void testViewsDoNotCount() {
        var project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .views(Map.of(".ml-anomalies-custom", new View(".ml-anomalies-custom", "FROM x", null, false)))
            .build();
        assertThat(MlAnomaliesView.hasResultsIndex(project), is(false));
    }

    private static IndexMetadata.Builder index(String name) {
        return IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), 1, 0));
    }
}
