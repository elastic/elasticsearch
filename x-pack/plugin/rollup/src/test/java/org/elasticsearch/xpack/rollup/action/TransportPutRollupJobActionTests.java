/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
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
            var metadata = createMetadata(mappings);
            assertTrue(TransportPutRollupJobAction.hasRollupIndices(metadata));
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
            var metadata = createMetadata(mappings);
            assertTrue(TransportPutRollupJobAction.hasRollupIndices(metadata));
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
            var metadata = createMetadata(mappings);
            assertTrue(TransportPutRollupJobAction.hasRollupIndices(metadata));
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
            var metadata = createMetadata(mappings);
            assertFalse(TransportPutRollupJobAction.hasRollupIndices(metadata));
        }
        {
            String mappings = """
                {
                    "_doc": {
                    }
                }
                """;
            var metadata = createMetadata(mappings);
            assertFalse(TransportPutRollupJobAction.hasRollupIndices(metadata));
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
            var metadata = createMetadata(mappings);
            assertFalse(TransportPutRollupJobAction.hasRollupIndices(metadata));
        }
    }

    private static Metadata createMetadata(String mappings) {
        Settings.Builder b = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current());
        var metadata = Metadata.builder()
            .put(IndexMetadata.builder("my-rollup-index").settings(b).numberOfShards(1).numberOfReplicas(0).putMapping(mappings))
            .build();
        return metadata;
    }

}
