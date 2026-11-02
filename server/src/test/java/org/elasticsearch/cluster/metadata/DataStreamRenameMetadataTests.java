/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Renaming a backing index must leave the project metadata self-consistent: the data stream must point at the index under its new
 * name, and wildcard resolution over the whole project must still find every index.
 */
public class DataStreamRenameMetadataTests extends ESTestCase {

    public void testWildcardResolutionAfterRenamingBackingIndex() {
        final String dataStreamName = "metrics-foo";
        final IndexMetadata first = DataStreamTestHelper.createBackingIndex(dataStreamName, 1).build();
        final IndexMetadata second = DataStreamTestHelper.createBackingIndex(dataStreamName, 2).build();
        final DataStream dataStream = DataStreamTestHelper.newInstance(dataStreamName, List.of(first.getIndex(), second.getIndex()));
        final ProjectMetadata before = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(first, false)
            .put(second, false)
            .put(dataStream)
            .build();

        final Index toRename = randomBoolean() ? first.getIndex() : second.getIndex();
        final Index renamed = new Index(toRename.getName() + "-renamed", toRename.getUUID());
        final IndexMetadata renamedMetadata = IndexMetadata.builder(before.index(toRename)).index(renamed.getName()).build();
        final ProjectMetadata after = ProjectMetadata.builder(before)
            .put(renamedMetadata, false)
            .remove(toRename.getName())
            .put(before.dataStreams().get(dataStreamName).renameIndex(toRename, renamed))
            .build();

        assertNull(after.index(toRename.getName()));
        assertNotNull(after.index(renamed));
        assertThat(
            after.dataStreams().get(dataStreamName).getIndices().stream().map(Index::getName).toList(),
            containsInAnyOrder(
                toRename == first.getIndex() ? renamed.getName() : first.getIndex().getName(),
                toRename == second.getIndex() ? renamed.getName() : second.getIndex().getName()
            )
        );
        final String[] resolved = TestIndexNameExpressionResolver.newInstance()
            .concreteIndexNames(after, IndicesOptions.lenientExpandOpenHidden(), "*");
        assertThat(
            List.of(resolved),
            containsInAnyOrder(
                toRename == first.getIndex() ? renamed.getName() : first.getIndex().getName(),
                toRename == second.getIndex() ? renamed.getName() : second.getIndex().getName()
            )
        );
    }
}
