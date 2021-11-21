/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.Releasable;

public class RequestMemory {

    private final Releasable coordinatingMemory;

    public RequestMemory() {
        this.coordinatingMemory = Releasable.NO_OP;
    }

    public RequestMemory(Releasable coordinatingMemory) {
        this.coordinatingMemory = coordinatingMemory;
    }

    public void releaseCoordinatingMemory() {
        coordinatingMemory.close();
    }

    public Releasable childCoordinatingMemoryReleaser(int numberOfChildRequests) {
        CountDown countDown = new CountDown(numberOfChildRequests);
        return () -> {
            if (countDown.countDown()) {
                coordinatingMemory.close();
            }
        };
    }
}
