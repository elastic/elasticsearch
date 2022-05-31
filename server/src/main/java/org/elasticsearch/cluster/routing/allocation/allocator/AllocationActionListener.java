/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

import java.util.concurrent.atomic.AtomicInteger;

public class AllocationActionListener {

    private final ActionListener<AcknowledgedResponse> delegate;
    private final SetOnce<AcknowledgedResponse> response = new SetOnce<>();
    private final AtomicInteger listenersExecuted = new AtomicInteger(2);

    public AllocationActionListener(ActionListener<AcknowledgedResponse> delegate) {
        this.delegate = delegate;
    }

    public ActionListener<AcknowledgedResponse> clusterStateUpdate() {
        return new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                response.set(acknowledgedResponse);
                if (listenersExecuted.decrementAndGet() == 0) {
                    delegate.onResponse(response.get());
                }
            }

            @Override
            public void onFailure(Exception e) {
                delegate.onFailure(e);
            }
        };
    }

    public ActionListener<Void> rereoute() {
        return new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                if (listenersExecuted.decrementAndGet() == 0) {
                    delegate.onResponse(response.get());
                }
            }

            @Override
            public void onFailure(Exception e) {
                delegate.onFailure(e);
            }
        };
    }
}
