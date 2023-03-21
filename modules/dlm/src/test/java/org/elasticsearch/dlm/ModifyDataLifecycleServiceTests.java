/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataLifecycle;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.dlm.DataStreamFactory.createDataStream;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class ModifyDataLifecycleServiceTests extends ESTestCase {

    private final ModifyDataLifecycleService service = new ModifyDataLifecycleService(mock(ClusterService.class));

    public void testModifyingLifecycle() {
        String dataStream = randomAlphaOfLength(5);
        DataLifecycle dataLifecycle = new DataLifecycle(randomMillisUpToYear9999());
        ClusterState before = addDataStream(ClusterState.builder(ClusterName.DEFAULT).build(), dataStream);
        {
            // Remove lifecycle
            ClusterState after = service.modifyLifecycle(before, List.of(dataStream), null);
            DataStream updatedDataStream = after.metadata().dataStreams().get(dataStream);
            assertNotNull(updatedDataStream);
            assertThat(updatedDataStream.getLifecycle(), nullValue());
            before = after;
        }

        {
            // Set lifecycle
            ClusterState after = service.modifyLifecycle(before, List.of(dataStream), dataLifecycle);
            DataStream updatedDataStream = after.metadata().dataStreams().get(dataStream);
            assertNotNull(updatedDataStream);
            assertThat(updatedDataStream.getLifecycle(), equalTo(dataLifecycle));
        }
    }

    private ClusterState addDataStream(ClusterState state, String dataStreamName) {
        int numBackingIndices = 3;
        Metadata.Builder builder = Metadata.builder();
        DataStream dataStream = createDataStream(
            builder,
            dataStreamName,
            numBackingIndices,
            Settings.builder().put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy").put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT),
            null,
            System.currentTimeMillis()
        );
        builder.put(dataStream);
        return ClusterState.builder(state).metadata(builder).build();
    }
}
