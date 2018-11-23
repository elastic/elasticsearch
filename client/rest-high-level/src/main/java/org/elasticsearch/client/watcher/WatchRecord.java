package org.elasticsearch.client.watcher;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;

import java.util.Map;

public class WatchRecord {

    public static final ParseField WATCH_ID = new ParseField("watch_id");
    public static final ParseField NODE = new ParseField("node");
    public static final ParseField MESSAGES = new ParseField("messages");
    public static final ParseField TRIGGER_EVENT = new ParseField("trigger_event");
    public static final ParseField STATUS = new ParseField("status");
    public static final ParseField STATE = new ParseField("state");
    public static final ParseField INPUT = new ParseField("input");
    public static final ParseField CONDITION = new ParseField("condition");
    public static final ParseField RESULT = new ParseField("result");

    private final String id;
    private final String node;
    private final String[] messages;
    private final Map<String, Object> triggerEvent;
    private final ExecutionState state;
    private final WatchStatus status;
    private final Map<String, Object> input;
    private final Map<String, Object> condition;
    private final Map<String, Object> result;

    public WatchRecord(String id, String node, String[] messages, Map<String, Object> triggerEvent,
                       ExecutionState state, WatchStatus status, Map<String, Object> input,
                       Map<String, Object> condition, Map<String, Object> result) {
        this.id = id;
        this.node = node;
        this.messages = messages;
        this.triggerEvent = triggerEvent;
        this.state = state;
        this.status = status;
        this.input = input;
        this.condition = condition;
        this.result = result;
    }

    public String getId() {
        return id;
    }

    public String getNode() {
        return node;
    }

    public String[] getMessages() {
        return messages;
    }

    public Map<String, Object> getTriggerEvent() {
        return triggerEvent;
    }

    public ExecutionState getState() {
        return state;
    }

    public WatchStatus getStatus() {
        return status;
    }

    public Map<String, Object> getInput() {
        return input;
    }

    public Map<String, Object> getCondition() {
        return condition;
    }

    public Map<String, Object> getResult() {
        return result;
    }

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<WatchRecord, Void> PARSER =
        new ConstructingObjectParser<>("watch_record", false,
            (fields) -> {
                String id = (String) fields[0];
                String node = (String) fields[1];
                String[] messages = (String[]) fields[2];
                Map<String, Object> triggerEvent = (Map<String, Object>) fields[3];
                ExecutionState state = (ExecutionState) fields[4];
                WatchStatus status = (WatchStatus) fields[5];
                Map<String, Object> input = (Map<String, Object>) fields[6];
                Map<String, Object> condition = (Map<String, Object>) fields[7];
                Map<String, Object> result = (Map<String, Object>) fields[8];
                return new WatchRecord(id, node, messages, triggerEvent, state, status, input, condition, result);
            });
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), WATCH_ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), MESSAGES);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), TRIGGER_EVENT);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> ExecutionState.resolve(p.text()), STATE);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> WatchStatus.parse(p), STATUS);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), INPUT);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), CONDITION);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), RESULT);
    }
}
