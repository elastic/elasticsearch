/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;

import java.io.IOException;

import static org.elasticsearch.common.regex.Regex.simpleMatch;
import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.util.CollectionUtils.isEmpty;

/**
 * A request to get node tasks
 */
public class ListTasksRequest extends BaseTasksRequest<ListTasksRequest> {

    public static final String[] ANY_DESCRIPTION = Strings.EMPTY_ARRAY;

    private boolean detailed = false;
    private boolean waitForCompletion = false;

    private String[] descriptions = ANY_DESCRIPTION;

    public ListTasksRequest() {
    }

    public ListTasksRequest(StreamInput in) throws IOException {
        super(in);
        detailed = in.readBoolean();
        waitForCompletion = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_7_13_0)) {
            descriptions = in.readStringArray();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(detailed);
        out.writeBoolean(waitForCompletion);
        if (out.getVersion().onOrAfter(Version.V_7_13_0)) {
            out.writeStringArray(descriptions);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (descriptions.length > 0 && detailed == false) {
            validationException = addValidationError("matching on descriptions is not available when [detailed] is false",
                validationException);
        }
        return validationException;
    }

    @Override
    public boolean match(Task task) {
        return super.match(task)
            && (isEmpty(getDescriptions()) || simpleMatch(getDescriptions(), task.getDescription()));
    }

    /**
     * Should the detailed task information be returned.
     */
    public boolean getDetailed() {
        return this.detailed;
    }

    /**
     * Should the detailed task information be returned.
     */
    public ListTasksRequest setDetailed(boolean detailed) {
        this.detailed = detailed;
        return this;
    }

    /**
     * Should this request wait for all found tasks to complete?
     */
    public boolean getWaitForCompletion() {
        return waitForCompletion;
    }

    /**
     * Should this request wait for all found tasks to complete?
     */
    public ListTasksRequest setWaitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

    /**
     * Description patters on which to match.
     *
     * If other matching criteria are set, descriptions are matched last once other criteria are satisfied
     *
     * Matching on descriptions is only available if `detailed` is `true`.
     * @return array of absolute or simple wildcard matching strings
     */
    public String[] getDescriptions() {
        return descriptions;
    }

    public ListTasksRequest setDescriptions(String... descriptions) {
        this.descriptions = descriptions;
        return this;
    }

}
