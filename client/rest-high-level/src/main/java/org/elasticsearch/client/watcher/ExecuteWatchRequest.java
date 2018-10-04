package org.elasticsearch.client.watcher;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
public class ExecuteWatchRequest {

    public enum ActionExecutionMode {
        SIMULATE, FORCE_SIMULATE, EXECUTE, FORCE_EXECUTE, SKIP
    }

    private final String id;
    private final BytesReference watchSource;

    private boolean ignoreCondition = false;
    private boolean recordExecution = false;
    private boolean debug = false;

    @Nullable
    private Map<String, Object> triggerData = null;
    @Nullable private Map<String, Object> alternativeInput = null;
    private Map<String, ActionExecutionMode> actionModes = new HashMap<>();

    public ExecuteWatchRequest(BytesReference watchSource) {
        this.id = null;
        this.watchSource = Objects.requireNonNull(watchSource, "Watch source cannot be null");
    }

    /**
     * @param id the id of the watch to execute
     */
    public ExecuteWatchRequest(String id) {
        this.id = Objects.requireNonNull(id, "Watch id cannot be null");
        this.watchSource = null;
    }

    /**
     * @return The id of the watch to be executed
     */
    public String getId() {
        return id;
    }

    /**
     * @return Should the condition for this execution be ignored
     */
    public boolean isIgnoreCondition() {
        return ignoreCondition;
    }

    /**
     * @param ignoreCondition set if the condition for this execution be ignored
     */
    public void setIgnoreCondition(boolean ignoreCondition) {
        this.ignoreCondition = ignoreCondition;
    }

    /**
     * @return Should this execution be recorded in the history index
     */
    public boolean isRecordExecution() {
        return recordExecution;
    }

    /**
     * @param recordExecution Sets if this execution be recorded in the history index
     */
    public void setRecordExecution(boolean recordExecution) {
        if (watchSource != null) {
            throw new IllegalArgumentException("The execution of an line watch cannot be recorded");
        }
        this.recordExecution = recordExecution;
    }

    /**
     * @return The alertnative input to use (may be null)
     */
    public Map<String, Object> getAlternativeInput() {
        return alternativeInput;
    }

    /**
     * @param alternativeInput Set's the alternative input
     */
    public void setAlternativeInput(Map<String, Object> alternativeInput) {
        this.alternativeInput = alternativeInput;
    }

    /**
     * @param data The data that should be associated with the trigger event.
     */
    public void setTriggerData(Map<String, Object> data) throws IOException {
        this.triggerData = data;
    }

    /**
     * @return the trigger to use
     */
    public Map<String, Object> getTriggerData() {
        return triggerData;
    }

    /**
     * @return the source of the watch to execute
     */
    public BytesReference getWatchSource() {
        return watchSource;
    }

    /**
     * @return  the execution modes for the actions. These modes determine the nature of the execution
     *          of the watch actions while the watch is executing.
     */
    public Map<String, ActionExecutionMode> getActionModes() {
        return actionModes;
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
     * @return whether the watch should execute in debug mode. In debug mode the execution {@code vars}
     *         will be returned as part of the watch record.
     */
    public boolean isDebug() {
        return debug;
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

