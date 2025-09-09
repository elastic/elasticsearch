/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

public class ProjectTagsTests extends ESTestCase {

    public void testValidTags() {
        ProjectId projectId = randomUniqueProjectId();
        ProjectTags.validateTags(projectId.id(), randomTags(projectId));
    }

    public void testMissingTags() {
        ProjectId projectId = randomUniqueProjectId();
        Map<String, String> tags = randomTags(projectId);
        tags.remove(randomFrom("_id", "_alias", "_type", "_organization"));
        var ex = expectThrows(IllegalStateException.class, () -> ProjectTags.validateTags(projectId.id(), tags));
        assertTrue(ex.getMessage().contains("missing required tag"));
    }

    Map<String, String> randomTags(ProjectId projectId) {
        return new HashMap<>(
            Map.of(
                "_id",
                projectId.id(),
                "_alias",
                randomAlphaOfLength(10),
                "_type",
                randomFrom("elasticsearch", "security", "observability"),
                "_organization",
                randomAlphaOfLength(10),
                "custom_tag_1",
                randomAlphaOfLength(5),
                "custom_tag_2",
                randomAlphaOfLength(5)
            )
        );
    }

}
