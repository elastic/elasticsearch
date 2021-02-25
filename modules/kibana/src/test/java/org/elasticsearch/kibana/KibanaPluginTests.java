/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.kibana;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class KibanaPluginTests extends ESTestCase {

    public void testKibanaIndexNames() {
        assertThat(new KibanaPlugin().getSettings(), contains(KibanaPlugin.KIBANA_INDEX_NAMES_SETTING));
        assertThat(
            new KibanaPlugin().getSystemIndexDescriptors(Settings.EMPTY)
                .stream()
                .map(SystemIndexDescriptor::getIndexPattern)
                .collect(Collectors.toUnmodifiableList()),
            contains(".kibana", ".kibana_*", ".reporting-*", ".apm-agent-configuration", ".apm-custom-link")
        );

        final List<String> names = List.of("." + randomAlphaOfLength(4), "." + randomAlphaOfLength(5));
        final List<String> namesFromDescriptors = new KibanaPlugin().getSystemIndexDescriptors(
            Settings.builder().putList(KibanaPlugin.KIBANA_INDEX_NAMES_SETTING.getKey(), names).build()
        ).stream().map(SystemIndexDescriptor::getIndexPattern).collect(Collectors.toUnmodifiableList());
        assertThat(namesFromDescriptors, is(names));

        assertThat(
            new KibanaPlugin().getSystemIndexDescriptors(Settings.EMPTY)
                .stream()
                .anyMatch(systemIndexDescriptor -> systemIndexDescriptor.matchesIndexPattern(".kibana-event-log-7-1")),
            is(false)
        );
    }
}
