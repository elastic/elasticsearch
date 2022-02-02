/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.client.Validatable;

import java.util.Objects;

public final class PauseFollowRequest implements Validatable {

    private final String followerIndex;

    public PauseFollowRequest(String followerIndex) {
        this.followerIndex = Objects.requireNonNull(followerIndex);
    }

    public String getFollowerIndex() {
        return followerIndex;
    }
}
