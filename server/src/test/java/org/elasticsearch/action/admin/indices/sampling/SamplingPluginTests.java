/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.List;

public class SamplingPluginTests extends ESTestCase {

    public void testGetNamedXContent() {
        // Create an instance of the plugin
        Plugin plugin = new SamplingPlugin();

        // Get the list of NamedXContentRegistry entries
        List<NamedXContentRegistry.Entry> entries = plugin.getNamedXContent();

        // Assert that the list is not null and has the correct size
        assertNotNull(entries);
        assertEquals(2, entries.size());

        // Verify the first entry for SamplingMetadata
        NamedXContentRegistry.Entry samplingMetadataEntry = entries.get(0);
        assertEquals(SamplingMetadata.TYPE, samplingMetadataEntry.name.getPreferredName());
        assertEquals(Metadata.ProjectCustom.class, samplingMetadataEntry.categoryClass);

        // Verify the second entry for SamplingConfiguration
        NamedXContentRegistry.Entry samplingConfigEntry = entries.get(1);
        assertEquals(SamplingConfiguration.TYPE, samplingConfigEntry.name.getPreferredName());
        assertEquals(SamplingConfiguration.class, samplingConfigEntry.categoryClass);
    }
}
