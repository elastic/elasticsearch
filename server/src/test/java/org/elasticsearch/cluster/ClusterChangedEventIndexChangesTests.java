/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Which indices a cluster state update deleted and which it only renamed, as {@link ClusterChangedEvent} reports them: an index is
 * deleted when its UUID is gone, and renamed when its name is gone but its UUID is still there.
 */
public class ClusterChangedEventIndexChangesTests extends ESTestCase {

    private static IndexMetadata index(String name, String uuid) {
        return IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), uuid, 1, 0)).build();
    }

    private static ClusterState stateOf(Map<String, String> uuidByName) {
        final ProjectMetadata.Builder project = ProjectMetadata.builder(ProjectId.DEFAULT);
        uuidByName.forEach((name, uuid) -> project.put(index(name, uuid), false));
        return ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(project).build();
    }

    /** Random renames, swaps, deletions and creations, checked against the definition applied to every index one by one. */
    public void testDeletedAndRenamedAgreeWithTheDefinition() {
        for (int round = 0; round < 50; round++) {
            final Map<String, String> previous = new HashMap<>();
            final int count = randomIntBetween(1, 40);
            for (int i = 0; i < count; i++) {
                previous.put("index-" + i, "uuid-" + i);
            }
            // the same indices, some gone, some under another name, some swapped, and new ones
            final Map<String, String> current = new HashMap<>();
            final List<String> uuids = new ArrayList<>(previous.values());
            final List<String> keep = new ArrayList<>();
            for (String uuid : uuids) {
                if (randomIntBetween(0, 3) != 0) {
                    keep.add(uuid);
                }
            }
            final List<String> names = new ArrayList<>(previous.keySet());
            for (String uuid : keep) {
                // an old name, which may belong to another index now, or a fresh one
                current.put(randomBoolean() ? randomFrom(names) : "renamed-" + uuid, uuid);
            }
            // two indices can't hold one name: the last assignment wins, so recompute what is actually present
            final Map<String, String> presentUuids = new HashMap<>(current);
            for (int i = 0; i < randomIntBetween(0, 3); i++) {
                presentUuids.put("new-" + i, "new-uuid-" + i);
            }

            final ClusterChangedEvent event = new ClusterChangedEvent("test", stateOf(presentUuids), stateOf(previous));

            final Set<String> uuidsNow = new HashSet<>(presentUuids.values());
            final Set<Index> expectedDeleted = new HashSet<>();
            final Map<String, String> expectedRenamed = new HashMap<>();
            for (Map.Entry<String, String> entry : previous.entrySet()) {
                final boolean sameNameAndUuid = entry.getValue().equals(presentUuids.get(entry.getKey()));
                if (uuidsNow.contains(entry.getValue()) == false) {
                    expectedDeleted.add(new Index(entry.getKey(), entry.getValue()));
                } else if (sameNameAndUuid == false) {
                    expectedRenamed.put(entry.getValue(), entry.getKey());
                }
            }

            assertThat(new HashSet<>(event.indicesDeleted()), equalTo(expectedDeleted));
            final Map<String, String> renamed = new HashMap<>();
            for (ClusterChangedEvent.RenamedIndex r : event.indicesRenamed()) {
                renamed.put(r.previous().getUUID(), r.previous().getName());
                assertThat(r.current().getIndexUUID(), equalTo(r.previous().getUUID()));
            }
            assertThat(renamed, equalTo(expectedRenamed));
        }
    }

    /** An update that deletes nothing and renames nothing reports nothing. */
    public void testNothingDeletedOrRenamed() {
        final Map<String, String> indices = Map.of("a", "uuid-a", "b", "uuid-b");
        final ClusterChangedEvent event = new ClusterChangedEvent("test", stateOf(indices), stateOf(Map.of("a", "uuid-a", "b", "uuid-b")));
        assertThat(event.indicesDeleted(), empty());
        assertThat(event.indicesRenamed(), empty());
    }

    /** The master asks for the renames twice per update, and a rescan of every index each time is the cost this avoids. */
    public void testRenamesAreComputedOnceForAnEvent() {
        final ClusterChangedEvent event = new ClusterChangedEvent(
            "test",
            stateOf(Map.of("renamed", "uuid-a", "b", "uuid-b")),
            stateOf(Map.of("a", "uuid-a", "b", "uuid-b"))
        );
        final List<ClusterChangedEvent.RenamedIndex> first = event.indicesRenamed();
        assertThat(first, hasSize(1));
        assertSame("the second call must not scan the indices again", first, event.indicesRenamed());
        assertThat(event.indicesDeleted(), empty());
    }
}
