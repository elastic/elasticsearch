/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.index.Index;

public interface LifecycleAction extends ToXContentObject, NamedWriteable {

    void execute(Index index, Client client, Listener listener);

    public static interface Listener {

        void onSuccess(boolean completed);

        void onFailure(Exception e);
    }
}
