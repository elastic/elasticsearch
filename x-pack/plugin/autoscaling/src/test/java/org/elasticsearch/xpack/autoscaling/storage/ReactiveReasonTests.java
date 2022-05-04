package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ReactiveReasonTests extends ESTestCase {

    public void testXContent() throws IOException {
        String reason = randomAlphaOfLength(10);
        long unassigned = randomNonNegativeLong();
        long assigned = randomNonNegativeLong();
        Set<ShardId> unassignedShardIds = randomUnique(() -> new ShardId("index", UUIDs.randomBase64UUID(), randomInt(5)), 8);
        Set<ShardId> assignedShardIds = randomUnique(() -> new ShardId("index", UUIDs.randomBase64UUID(), randomInt(5)), 8);
        var reactiveReason = new ReactiveStorageDeciderService.ReactiveReason(
            reason,
            unassigned,
            assigned,
            unassignedShardIds,
            assignedShardIds
        );

        try (
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                BytesReference.bytes(reactiveReason.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            )
        ) {
            Map<String, Object> map = parser.map();
            assertEquals(reason, map.get("reason"));
            assertEquals(unassigned, map.get("unassigned"));
            assertEquals(assigned, map.get("assigned"));
            assertEquals(unassignedShardIds.stream().map(ShardId::toString).collect(Collectors.toList()), map.get("unassigned_shard_ids"));
            assertEquals(assignedShardIds.stream().map(ShardId::toString).collect(Collectors.toList()), map.get("assigned_shard_ids"));
        }
    }
}
