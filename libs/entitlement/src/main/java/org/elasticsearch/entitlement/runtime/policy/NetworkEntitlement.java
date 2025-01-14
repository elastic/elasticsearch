/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.core.Strings;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import static java.util.Map.entry;

/**
 * Describes a network entitlement (sockets) with actions.
 */
public class NetworkEntitlement implements Entitlement {

    public static final int LISTEN_ACTION = 0x1;
    public static final int CONNECT_ACTION = 0x2;
    public static final int ACCEPT_ACTION = 0x4;

    static final String LISTEN = "listen";
    static final String CONNECT = "connect";
    static final String ACCEPT = "accept";

    private static final Map<String, Integer> ACTION_MAP = Map.ofEntries(
        entry(LISTEN, LISTEN_ACTION),
        entry(CONNECT, CONNECT_ACTION),
        entry(ACCEPT, ACCEPT_ACTION)
    );

    private final int actions;

    @ExternalEntitlement(parameterNames = { "actions" }, esModulesOnly = false)
    public NetworkEntitlement(List<String> actionsList) {

        int actionsInt = 0;

        for (String actionString : actionsList) {
            var action = ACTION_MAP.get(actionString);
            if (action == null) {
                throw new IllegalArgumentException("unknown network action [" + actionString + "]");
            }
            if ((actionsInt & action) == action) {
                throw new IllegalArgumentException(Strings.format("network action [%s] specified multiple times", actionString));
            }
            actionsInt |= action;
        }

        this.actions = actionsInt;
    }

    public NetworkEntitlement(int actions) {
        this.actions = actions;
    }

    public static String printActions(int actions) {
        var joiner = new StringJoiner(",");
        for (var entry : ACTION_MAP.entrySet()) {
            var action = entry.getValue();
            if ((actions & action) == action) {
                joiner.add(entry.getKey());
            }
        }
        return joiner.toString();
    }

    /**
     * For the actions to match, the actions present in this entitlement must be a superset
     * of the actions required by a check.
     * There is only one "negative" case (action required by the check but not present in the entitlement),
     * and it can be expressed efficiently via this truth table:
     * this.actions | requiredActions |
     * 0            | 0               | 0
     * 0            | 1               | 1 --> NOT this.action AND requiredActions
     * 1            | 0               | 0
     * 1            | 1               | 0
     *
     * @param requiredActions the actions required to be present for a check to pass
     * @return true if requiredActions are present, false otherwise
     */
    public boolean matchActions(int requiredActions) {
        return (~this.actions & requiredActions) == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NetworkEntitlement that = (NetworkEntitlement) o;
        return actions == that.actions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(actions);
    }

    @Override
    public String toString() {
        return "NetworkEntitlement{actions=" + actions + '}';
    }
}
