/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.action;

import org.elasticsearch.logging.Level;

import java.util.HashMap;
import java.util.Map;

/**
 * Message for action logging.
 */
public class ActionLogMessage extends HashMap<String, String> {
    private final Level level;

    public ActionLogMessage(Level level, Map<String, String> map) {
        super(map);
        this.level = level;
    }

    public Level level() {
        return level;
    }

    public void put(String key, Object value) {
        super.put(key, String.valueOf(value));
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ActionLogMessage == false) {
            return false;
        }
        return super.equals(o) && level == ((ActionLogMessage) o).level;
    }

    @Override
    public int hashCode() {
        return super.hashCode() * 31 + level.hashCode();
    }
}
