/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import java.util.List;
import java.util.Objects;

/**
 * Describes a file entitlement with a path and actions.
 */
public class FileEntitlement implements Entitlement {

    public static final int READ_ACTION = 0x1;
    public static final int WRITE_ACTION = 0x2;

    private final String path;
    private final int actions;

    @ExternalEntitlement(parameterNames = { "path", "actions" })
    public FileEntitlement(String path, List<String> actionsList) {
        this.path = path;
        int actionsInt = 0;

        for (String actionString : actionsList) {
            if ("read".equals(actionString)) {
                if ((actionsInt & READ_ACTION) == READ_ACTION) {
                    throw new IllegalArgumentException("file action [read] specified multiple times");
                }
                actionsInt |= READ_ACTION;
            } else if ("write".equals(actionString)) {
                if ((actionsInt & WRITE_ACTION) == WRITE_ACTION) {
                    throw new IllegalArgumentException("file action [write] specified multiple times");
                }
                actionsInt |= WRITE_ACTION;
            } else {
                throw new IllegalArgumentException("unknown file action [" + actionString + "]");
            }
        }

        this.actions = actionsInt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileEntitlement that = (FileEntitlement) o;
        return actions == that.actions && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, actions);
    }

    @Override
    public String toString() {
        return "FileEntitlement{" + "path='" + path + '\'' + ", actions=" + actions + '}';
    }
}
