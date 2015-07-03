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

package org.elasticsearch.plugin.probe.sigar;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;

import java.util.Arrays;
import java.util.Collection;

public class SigarPlugin extends AbstractPlugin {

    public static final String NAME = "sigar";
    public static final String ENABLED = NAME + ".enabled";
    public static final String BOOTSTRAP_ENABLED = "bootstrap." + NAME;

    private final boolean enabled;

    public SigarPlugin(Settings settings) {
        this.enabled = sigarEnabled(settings);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String description() {
        return "Elasticsearch Sigar Plugin";
    }

    @Override
    public Collection<Module> modules(Settings settings) {
        if (!enabled) {
            return ImmutableList.of();
        }
        return Arrays.asList((Module) new SigarModule(settings));
    }

    public static boolean sigarEnabled(Settings settings) {
        return settings.getAsBoolean(ENABLED, settings.getAsBoolean(BOOTSTRAP_ENABLED, true));
    }
}
