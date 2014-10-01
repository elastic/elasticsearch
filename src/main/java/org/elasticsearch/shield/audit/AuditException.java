/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit;

import org.elasticsearch.shield.ShieldException;

/**
 *
 */
public class AuditException extends ShieldException {

    public AuditException(String msg) {
        super(msg);
    }

    public AuditException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
