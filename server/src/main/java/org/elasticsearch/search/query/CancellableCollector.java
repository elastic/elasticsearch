/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.LeafCollector;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.util.function.BooleanSupplier;

/**
 * Collector that checks if the task it is executed under is cancelled.
 */
public class CancellableCollector extends FilterCollector {
    private final BooleanSupplier cancelled;

    /**
     * Constructor
     * @param cancelled supplier of the cancellation flag, the supplier will be called for each segment
     * @param in wrapped collector
     */
    public CancellableCollector(BooleanSupplier cancelled, Collector in) {
        super(in);
        this.cancelled = cancelled;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        if (cancelled.getAsBoolean()) {
            throw new TaskCancelledException("cancelled");
        }
        return super.getLeafCollector(context);
    }
}
