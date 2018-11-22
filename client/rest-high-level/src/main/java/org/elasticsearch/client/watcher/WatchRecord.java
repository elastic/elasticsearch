package org.elasticsearch.client.watcher;

import java.util.Map;

public class WatchRecord {

    private final String id;
    private final String node;
    private final String[] messages;
    private final TriggerEvent triggerEvent;
    private final ExecutionState state;
    private final WatchStatus status;
    private final Map<String, Object> input;

}
