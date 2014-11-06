/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.triggers.TriggerResult;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public interface AlertAction extends ToXContent {

    public String getActionName();

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException;

    public void writeTo(StreamOutput out) throws IOException;
    public void readFrom(StreamInput in) throws IOException;

    public boolean doAction(Alert alert, TriggerResult result);


}
