/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gateway;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MetaStateServiceTests extends ESTestCase {

    private NodeEnvironment env;
    private MetaStateService metaStateService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        env = newNodeEnvironment();
        metaStateService = new MetaStateService(env, xContentRegistry());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        env.close();
    }

    private static IndexMetadata indexMetadata(String name) {
        return IndexMetadata.builder(name)
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()))
            .build();
    }

    public void testWriteLoadIndex() throws Exception {
        IndexMetadata index = indexMetadata("test1");
        MetaStateWriterUtils.writeIndex(env, "test_write", index);
        assertThat(metaStateService.loadIndexState(index.getIndex()), equalTo(index));
    }

    public void testLoadMissingIndex() throws Exception {
        assertThat(metaStateService.loadIndexState(new Index("test1", "test1UUID")), nullValue());
    }
}
