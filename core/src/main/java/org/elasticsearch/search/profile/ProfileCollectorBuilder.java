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

package org.elasticsearch.search.profile;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.MultiCollector;

import java.util.AbstractList;
import java.util.Collections;
import java.util.List;

/**
 * Convenience object to help wrap collectors when profiling is enabled
 */
public class ProfileCollectorBuilder {

    private final boolean profile;
    private Collector currentCollector;

    public ProfileCollectorBuilder(boolean profile) {
        this.profile = profile;
    }

    /**
     * If profiling is enabled, this will wrap the Collector in an InternalProfileCollector
     * and record the hierarchy.  Otherwise it just returns the provided Collector
     *
     * @param collector  The collector to wrap
     * @param reason     The "hint" about what this collector is used for
     * @return           A wrapped collector if profiling is enabled, the original otherwise
     */
    public Collector wrap(Collector collector, String reason) {
        if (profile) {
            final List<InternalProfileCollector> children;
            if (currentCollector == null) {
                children = Collections.emptyList();
            } else {
                children = Collections.singletonList((InternalProfileCollector) currentCollector);
            }
            collector = new InternalProfileCollector(collector, reason, children);
            currentCollector = collector;
        }
        return collector;
    }

    /**
     * If profiling is enabled, this will wrap a MultiCollector in an InternalProfileCollector
     * and record the hierarchy.
     *
     * Because MultiCollector does not have any methods to retrieve it's wrapped sub-collectors,
     * the caller must provide those as an argument
     *
     * @param collector     The collector to wrap
     * @param reason        The "hint" about what this collector is used for
     * @param subCollectors The sub-collectors that are inside the multicollector
     * @return              A wrapped collector if profiling is enabled, the original otherwise
     */
    public Collector wrapMultiCollector(Collector collector, String reason, List<Collector> subCollectors) {
        if (!(collector instanceof MultiCollector)) {
            // When there is a single collector to wrap, MultiCollector returns it
            // directly, so use default wrapping logic for that case
            return wrap(collector, reason);
        }

        if (profile) {
            final List<InternalProfileCollector> children;
            if (currentCollector == null) {
                children = Collections.emptyList();
            } else {
                children = new AbstractList<InternalProfileCollector>() {
                    @Override
                    public InternalProfileCollector get(int index) {
                        return (InternalProfileCollector) subCollectors.get(index);
                    }
                    @Override
                    public int size() {
                        return subCollectors.size();
                    }
                };
            }
            collector = new InternalProfileCollector(collector, reason, children);
        }
        return collector;
    }
}

