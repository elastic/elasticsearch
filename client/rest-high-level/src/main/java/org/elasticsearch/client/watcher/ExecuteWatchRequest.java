package org.elasticsearch.client.watcher;

import org.apache.http.HttpEntity;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * An execute watch request to execute a watch by id
 */
public class ExecuteWatchRequest implements Validatable, ToXContentObject {

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    public enum ActionExecutionMode {
        SIMULATE, FORCE_SIMULATE, EXECUTE, FORCE_EXECUTE, SKIP
    }

    private final String id;

    private boolean ignoreCondition = false;
    private boolean recordExecution = false;
    private boolean debug = false;

    @Nullable
    private XContentBuilder triggerData = null;

    @Nullable
    private XContentBuilder alternativeInput = null;

    private Map<String, ActionExecutionMode> actionModes = new HashMap<>();

    /**
     * @param id the id of the watch to execute
     */
    public ExecuteWatchRequest(String id) {
        this.id = Objects.requireNonNull(id, "Watch id cannot be null");
    }

    /**
     * @param ignoreCondition set if the condition for this execution be ignored
     */
    public void setIgnoreCondition(boolean ignoreCondition) {
        this.ignoreCondition = ignoreCondition;
    }

    /**
     * @param recordExecution Sets if this execution be recorded in the history index
     */
    public void setRecordExecution(boolean recordExecution) {
        this.recordExecution = recordExecution;
    }

    /**
     * @param alternativeInput Set's the alternative input
     */
    public void setAlternativeInput(XContentBuilder alternativeInput) {
        this.alternativeInput = alternativeInput;
    }

    /**
     * @param data The data that should be associated with the trigger event.
     */
    public void setTriggerData(XContentBuilder data) throws IOException {
        this.triggerData = data;
    }

    /**
     * Sets the action execution mode for the give action (identified by its id).
     *
     * @param actionId      the action id.
     * @param actionMode    the execution mode of the action.
     */
    public void setActionMode(String actionId, ActionExecutionMode actionMode) {
        Objects.requireNonNull(actionId, "actionId cannot be null");
        actionModes.put(actionId, actionMode);
    }

    /**
     * @param debug indicates whether the watch should execute in debug mode. In debug mode the
     *              returned watch record will hold the execution {@code vars}
     */
    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    @Override
    public String toString() {
        return "execute[" + id + "]";
    }
}

