package org.elasticsearch.action.search;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

public class MainSearchTaskStatusTests extends ESTestCase {

    public void testMainSearchTaskStatusToXContent() {
        MainSearchTaskStatus status = new MainSearchTaskStatus();
        status.phaseStarted("phase1", 10);
        SearchShardTarget target1 = new SearchShardTarget("node", new ShardId("index", "indexUUID", 0), null, OriginalIndices.NONE);
        status.shardFailed("phase1", target1, new UnsupportedOperationException());
        SearchShardTarget target2 = new SearchShardTarget("node", new ShardId("index", "indexUUID", 1), null, OriginalIndices.NONE);
        TaskInfo taskInfo = new TaskInfo(new TaskId("node", 0), "type", "action", null, null, -1, -1,
            true, TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        SearchPhaseResult searchPhaseResult = new SearchPhaseResult() {};
        searchPhaseResult.setSearchShardTarget(target2);
        searchPhaseResult.setTaskInfo(taskInfo);
        status.shardProcessed("phase1", searchPhaseResult);
        status.phaseCompleted("phase1");

        status.phaseStarted("phase2", 5);
        status.phaseFailed("phase2", new RuntimeException("error"));

        status.phaseStarted("phase3", 1);

        String string = Strings.toString(status, true, true);

        assertEquals("{\n" +
            "  \"phase_running\" : {\n" +
            "    \"phase3\" : {\n" +
            "      \"total_shards\" : 1,\n" +
            "      \"processed_shards\" : 0,\n" +
            "      \"processed\" : [ ]\n" +
            "    }\n" +
            "  },\n" +
            "  \"phases_completed\" : [\n" +
            "    {\n" +
            "      \"phase1\" : {\n" +
            "        \"total_shards\" : 10,\n" +
            "        \"processed_shards\" : 2,\n" +
            "        \"processed\" : [\n" +
            "          {\n" +
            "            \"index_name\" : \"index\",\n" +
            "            \"index_uuid\" : \"indexUUID\",\n" +
            "            \"shard_id\" : 0,\n" +
            "            \"node_id\" : \"node\",\n" +
            "            \"failure\" : {\n" +
            "              \"error\" : {\n" +
            "                \"root_cause\" : [\n" +
            "                  {\n" +
            "                    \"type\" : \"unsupported_operation_exception\",\n" +
            "                    \"reason\" : null\n" +
            "                  }\n" +
            "                ],\n" +
            "                \"type\" : \"unsupported_operation_exception\",\n" +
            "                \"reason\" : null\n" +
            "              }\n" +
            "            }\n" +
            "          },\n" +
            "          {\n" +
            "            \"index_name\" : \"index\",\n" +
            "            \"index_uuid\" : \"indexUUID\",\n" +
            "            \"shard_id\" : 1,\n" +
            "            \"node_id\" : \"node\",\n" +
            "            \"task_info\" : {\n" +
            "              \"node\" : \"node\",\n" +
            "              \"id\" : 0,\n" +
            "              \"type\" : \"type\",\n" +
            "              \"action\" : \"action\",\n" +
            "              \"start_time\" : \"1969-12-31T23:59:59.999Z\",\n" +
            "              \"start_time_in_millis\" : -1,\n" +
            "              \"running_time\" : \"-1\",\n" +
            "              \"running_time_in_nanos\" : -1,\n" +
            "              \"cancellable\" : true,\n" +
            "              \"headers\" : { }\n" +
            "            }\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"phase2\" : {\n" +
            "        \"total_shards\" : 5,\n" +
            "        \"processed_shards\" : 0,\n" +
            "        \"processed\" : [ ],\n" +
            "        \"failure\" : {\n" +
            "          \"type\" : \"runtime_exception\",\n" +
            "          \"reason\" : \"error\"\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}", string);
    }

    public void testSerialization() throws IOException {
        MainSearchTaskStatus status = new MainSearchTaskStatus();
        if (randomBoolean()) {
            String phase = randomAlphaOfLengthBetween(5, 10);
            status.phaseStarted(phase, 1);
            if (randomBoolean()) {
                SearchShardTarget target1 = new SearchShardTarget("node", new ShardId("index", "indexUUID", 0), null, OriginalIndices.NONE);
                status.shardFailed(phase, target1, new IllegalArgumentException("test"));
            }
            if (randomBoolean()) {
                SearchShardTarget target2 = new SearchShardTarget("node", new ShardId("index", "indexUUID", 1), null, OriginalIndices.NONE);
                TaskInfo taskInfo = new TaskInfo(new TaskId("node", 0), "type", "action", null, null, -1, -1,
                    true, TaskId.EMPTY_TASK_ID, Collections.emptyMap());
                SearchPhaseResult searchPhaseResult = new SearchPhaseResult() {};
                searchPhaseResult.setSearchShardTarget(target2);
                searchPhaseResult.setTaskInfo(taskInfo);
                status.shardProcessed(phase, searchPhaseResult);
            }
            status.phaseCompleted(phase);
        }
        if (randomBoolean()) {
            String phase = randomAlphaOfLengthBetween(5, 10);
            status.phaseStarted(phase, 5);
            status.phaseFailed(phase, new SearchPhaseExecutionException(phase, "error", ShardSearchFailure.EMPTY_ARRAY));
        }
        if (randomBoolean()) {
            String phase = randomAlphaOfLengthBetween(5, 10);
            status.phaseStarted(phase, 1);
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            status.writeTo(out);
            try (StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes)) {
                MainSearchTaskStatus deserialized = new MainSearchTaskStatus(in);
                if (status.getCurrentPhase() == null) {
                    assertNull(deserialized.getCurrentPhase());
                } else {
                    assertNotNull(deserialized.getCurrentPhase());
                    MainSearchTaskStatus.PhaseInfo phase = status.getCurrentPhase();
                    MainSearchTaskStatus.PhaseInfo deserializedPhase = deserialized.getCurrentPhase();
                    assertEqualPhases(phase, deserializedPhase);
                }
                assertEquals(status.getCompletedPhases().size(), deserialized.getCompletedPhases().size());
                Iterator<MainSearchTaskStatus.PhaseInfo> iterator = deserialized.getCompletedPhases().iterator();
                for (MainSearchTaskStatus.PhaseInfo completedPhase : status.getCompletedPhases()) {
                    MainSearchTaskStatus.PhaseInfo deserializedCompletedPhase = iterator.next();
                    assertEqualPhases(completedPhase, deserializedCompletedPhase);
                }
            }
        }
    }

    private static void assertEqualPhases(MainSearchTaskStatus.PhaseInfo phase, MainSearchTaskStatus.PhaseInfo deserializedPhase) {
        assertEquals(phase.getExpectedOps(), deserializedPhase.getExpectedOps());
        assertEquals(phase.getProcessedShards().size(), deserializedPhase.getProcessedShards().size());
        if (phase.getFailure() == null) {
            assertNull(deserializedPhase.getFailure());
        } else {
            assertEquals(phase.getFailure().getClass(), deserializedPhase.getFailure().getClass());
        }
        Iterator<MainSearchTaskStatus.ShardInfo> iterator = deserializedPhase.getProcessedShards().iterator();
        for (MainSearchTaskStatus.ShardInfo shard : phase.getProcessedShards()) {
            MainSearchTaskStatus.ShardInfo deserializedShard = iterator.next();
            assertEquals(shard.getSearchShardTarget(), deserializedShard.getSearchShardTarget());
            if (shard.getFailure() == null) {
                assertEquals(shard.getTaskInfo(), deserializedShard.getTaskInfo());
                assertNull(deserializedShard.getFailure());
            } else {
                assertNull(deserializedShard.getTaskInfo());
                assertEquals(shard.getFailure().getClass(), deserializedShard.getFailure().getClass());
            }
        }
    }
}
