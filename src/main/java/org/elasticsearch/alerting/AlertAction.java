/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public interface AlertAction extends ToXContent {
    public String getActionName();
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException;

    public boolean doAction(String alertName, AlertResult alert);
}
