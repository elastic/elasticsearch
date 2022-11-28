/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.user;

import java.util.Map;

/**
 * A lightweight representation of the "user" that is executing the current action
 */
public interface ActionUser {

    /**
     * The identifier for this user
     */
    interface Id {
        /**
         * A plain string representation of this user identifier
         */
        String toString();

        /**
         * A Map representation of this user, suitable for turning into JSON
         */
        Map<String, Object> asMap();
    }

    Id identifier();
}
