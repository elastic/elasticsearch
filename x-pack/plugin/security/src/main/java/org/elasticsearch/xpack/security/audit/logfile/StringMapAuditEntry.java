/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.apache.logging.log4j.message.StringMapMessage;
import org.elasticsearch.xpack.core.security.audit.AuditEntry;

final class StringMapAuditEntry implements AuditEntry {

    private final StringMapMessage message;

    StringMapAuditEntry(StringMapMessage message) {
        this.message = message;
    }

    @Override
    public String get(String field) {
        return message.get(field);
    }

    @Override
    public AuditEntry set(String field, String value) {
        message.with(field, value);
        return this;
    }
}
