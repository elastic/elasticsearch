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

package org.elasticsearch.monitor.probe;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class ProbeRegistry<P extends Probe> {

    private final Map<String, P> probes;

    public ProbeRegistry(Map<String, P> probes) {
        this.probes = ImmutableMap.copyOf(probes);
    }

    public abstract P probe();

    public P probe(String type) {
        return probes.get(type);
    }

    public Collection<P> probes(String... types) {
        if (CollectionUtils.isEmpty(types)) {
            return probes.values();
        }

        List<P> orderedProbes = new ArrayList<>(types.length);
        for (String type : types) {
            P probe = probe(type);
            if (probe != null) {
                orderedProbes.add(probe);
            }
        }
        return orderedProbes;
    }

    public String[] types() {
        return Strings.toStringArray(probes.keySet());
    }
}
