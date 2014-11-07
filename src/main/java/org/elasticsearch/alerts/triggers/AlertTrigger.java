/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.triggers;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.ToXContent;

public interface AlertTrigger extends ToXContent {

    /**
     * Returns the name of this trigger, can be used to look up the action factory for creating this trigger
     * @return
     */
    public String getTriggerName();
}
