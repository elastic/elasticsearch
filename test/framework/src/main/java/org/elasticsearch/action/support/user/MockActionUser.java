/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.user;

import java.util.Map;

public record MockActionUser(String username) implements ActionUser {

    @Override
    public Id identifier() {
        return new Id() {

            @Override
            public String toString() {
                return "MOCK:" + username;
            }

            @Override
            public Map<String, String> asEcsMap() {
                return Map.of("user.name", username);
            }
        };
    }
}
