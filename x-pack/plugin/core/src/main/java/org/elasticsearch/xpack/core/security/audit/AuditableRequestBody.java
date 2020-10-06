/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.audit;

import org.apache.logging.log4j.message.StringMapMessage;

public interface AuditableRequestBody {
    // changing any of these field names requires changing the log4j2.properties file too
    String EVENT_ACTION_FIELD_NAME = "event.action";
    String EVENT_ACTION_DETAILS_FIELD_NAME = "event.action.details";

    void audit(StringMapMessage auditMessage);
}
