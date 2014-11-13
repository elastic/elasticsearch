/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.common.xcontent.ToXContent;

/**
 * Classes that implement this interface should be a POJO
 * containing the data needed to do this action
 */
public interface AlertAction extends ToXContent {

    /**
     *
     * @return
     */
    public String getActionName();

}
