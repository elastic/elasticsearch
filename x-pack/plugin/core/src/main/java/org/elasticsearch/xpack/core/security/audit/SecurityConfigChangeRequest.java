/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.audit;

import java.util.Map;

/**
 * Requests may implement this interface to emit {@code security_config_change} audit events (note that they need to be included in
 * {@code LoggingAuditTrail#SECURITY_CHANGE_ACTIONS} as well).
 * Any sensitive information included in the request must not be included in the audit log, and these fields should be redacted when
 * provided through this interface.
 */
public interface SecurityConfigChangeRequest {
    Map<String, Object> auditFields();
}
