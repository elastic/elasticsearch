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
package org.elasticsearch.common.settings;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 */
public class SettingsFilter extends AbstractComponent {

    public static interface Filter {

        void filter(ImmutableSettings.Builder settings);
    }

    private final CopyOnWriteArrayList<Filter> filters = new CopyOnWriteArrayList<>();

    @Inject
    public SettingsFilter(Settings settings) {
        super(settings);
    }

    public void addFilter(Filter filter) {
        filters.add(filter);
    }

    public void removeFilter(Filter filter) {
        filters.remove(filter);
    }

    public Settings filterSettings(Settings settings) {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder().put(settings);
        for (Filter filter : filters) {
            filter.filter(builder);
        }
        return builder.build();
    }
}
