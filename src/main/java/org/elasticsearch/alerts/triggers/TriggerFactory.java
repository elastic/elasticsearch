/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.triggers;


import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

public interface TriggerFactory {

    /**
     * Creates a trigger form the given parser
     * @param parser The parser containing the definition of the trigger
     * @return The newly created trigger
     * @throws IOException
     */
    AlertTrigger createTrigger(XContentParser parser) throws IOException;

    /**
     * Evaulates if the trigger is triggers based off of the request and response
     *
     * @param trigger
     * @param request
     * @param response
     * @return
     */
    boolean isTriggered(AlertTrigger trigger, SearchRequest request, Map<String, Object> response);

}
