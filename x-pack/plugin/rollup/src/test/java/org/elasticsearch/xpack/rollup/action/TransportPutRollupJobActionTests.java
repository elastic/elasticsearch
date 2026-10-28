/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class TransportPutRollupJobActionTests extends ESTestCase {

    public void testHasRollupIndices() throws IOException {
        {
            String mappings = """
                {
                    "_doc": {
                        "properties": {
                            "field1": {
                                "type": "long"
                            }
                        },
                        "_meta": {
                            "_rollup": {
                                "job_id": {}
                            }
                        }
                    }
                }
                """;
            var project = createProject(mappings);
            assertTrue(TransportPutRollupJobAction.hasRollupIndices(project));
        }
        {
            String mappings = """
                {
                    "_doc": {
                        "properties": {
                            "field1": {
                                "type": "long"
                            }
                        },
                        "_meta": {
                            "_rollup": {
                                "job_id": {}
                            }
                        }
                    }
                }
                """;
            var project = createProject(mappings);
            assertTrue(TransportPutRollupJobAction.hasRollupIndices(project));
        }
        {
            String mappings = """
                {
                    "_doc": {
                        "properties": {
                            "field1": {
                                "type": "long"
                            }
                        },
                        "_meta": {
                            "other_metadata": {},
                            "_rollup": {
                                "job_id": {}
                            }
                        }
                    }
                }
                """;
            var project = createProject(mappings);
            assertTrue(TransportPutRollupJobAction.hasRollupIndices(project));
        }
        {
            String mappings = """
                {
                    "_doc": {
                        "properties": {
                            "field1": {
                                "type": "long"
                            }
                        }
                    }
                }
                """;
            var project = createProject(mappings);
            assertFalse(TransportPutRollupJobAction.hasRollupIndices(project));
        }
        {
            String mappings = """
                {
                    "_doc": {
                    }
                }
                """;
            var project = createProject(mappings);
            assertFalse(TransportPutRollupJobAction.hasRollupIndices(project));
        }
        {
            String mappings = """
                {
                    "_doc": {
                        "properties": {
                            "field1": {
                                "type": "long"
                            }
                        },
                        "_meta": {
                            "other_metadata": {}
                        }
                    }
                }
                """;
            var project = createProject(mappings);
            assertFalse(TransportPutRollupJobAction.hasRollupIndices(project));
        }
    }

    private static ProjectMetadata createProject(String mappings) {
        return ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(IndexMetadata.builder("my-rollup-index").settings(indexSettings(IndexVersion.current(), 1, 0)).putMapping(mappings))
            .build();
    }

}
