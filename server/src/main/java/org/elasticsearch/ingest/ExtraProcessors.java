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

package org.elasticsearch.ingest;

import org.elasticsearch.common.Nullable;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;

public final class ExtraProcessors {

    private final Map<String, Processor> processors;

    public static ExtraProcessors create(@Nullable Map<String, Map<String, Map<String, Object>>> processorsConfig, String tag,
                                  Map<String, Processor.Factory> registry) {
        Map<String, Processor> processors = new HashMap<>();
        Optional.ofNullable(processorsConfig).orElse(Collections.emptyMap()).forEach((name, config) -> {
            Set<Map.Entry<String, Map<String, Object>>> entries = config.entrySet();
            if (entries.size() != 1) {
                throw newConfigurationException(
                    "script", tag, "extra_processors", "Extra processors child must specify exactly one processor type"
                );
            }
            Map.Entry<String, Map<String, Object>> entry = entries.iterator().next();
            try {
                processors.put(
                    name,
                    ConfigurationUtils.readProcessor(registry, entry.getKey(), entry.getValue())
                );
            } catch (Exception ex) {
                throw new IllegalArgumentException(ex);
            }
        });
        return new ExtraProcessors(processors);
    }

    public ExtraProcessors(Map<String, Processor> processors) {
        this.processors = processors;
    }

    public void invoke(String name, Object document) {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                processors.get(name).execute((IngestDocument) document);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            return null;
        });
    }
}
