/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collections;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class ReservedPipelineActionTests extends ESTestCase {

    private TransformState<ProjectMetadata> processJSON(
        ReservedPipelineAction action,
        TransformState<ProjectMetadata> prevState,
        String json
    ) throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return action.transform(action.fromXContent(parser), prevState);
        }
    }

    public void testAddRemoveIngestPipeline() throws Exception {
        ProjectMetadata state = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        TransformState<ProjectMetadata> prevState = new TransformState<>(state, Collections.emptySet());
        ReservedPipelineAction action = new ReservedPipelineAction();

        String emptyJSON = "";

        TransformState<ProjectMetadata> updatedState = processJSON(action, prevState, emptyJSON);
        assertThat(updatedState.keys(), empty());
        assertEquals(prevState.state(), updatedState.state());

        String json = """
            {
               "my_ingest_pipeline": {
                   "description": "_description",
                   "processors": [
                      {
                        "set" : {
                          "field": "_field",
                          "value": "_value"
                        }
                      }
                   ]
               },
               "my_ingest_pipeline_1": {
                   "description": "_description",
                   "processors": [
                      {
                        "set" : {
                          "field": "_field",
                          "value": "_value"
                        }
                      }
                   ]
               }
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, json);
        assertThat(updatedState.keys(), containsInAnyOrder("my_ingest_pipeline", "my_ingest_pipeline_1"));

        String halfJSON = """
            {
               "my_ingest_pipeline_1": {
                   "description": "_description",
                   "processors": [
                      {
                        "set" : {
                          "field": "_field",
                          "value": "_value"
                        }
                      }
                   ]
               }
            }""";

        updatedState = processJSON(action, prevState, halfJSON);
        assertThat(updatedState.keys(), containsInAnyOrder("my_ingest_pipeline_1"));

        updatedState = processJSON(action, prevState, emptyJSON);
        assertThat(updatedState.keys(), empty());
    }
}
