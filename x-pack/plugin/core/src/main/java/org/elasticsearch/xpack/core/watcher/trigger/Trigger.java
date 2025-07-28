/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.trigger;

import org.elasticsearch.xcontent.ToXContentObject;

public interface Trigger extends ToXContentObject {

    String type();

    interface Builder<T extends Trigger> {

        T build();
    }

}
