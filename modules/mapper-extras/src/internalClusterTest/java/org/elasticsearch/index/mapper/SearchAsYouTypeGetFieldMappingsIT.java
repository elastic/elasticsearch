/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.nullValue;

public class SearchAsYouTypeGetFieldMappingsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MapperExtrasPlugin.class);
    }

    /**
     * A search_as_you_type field has mappers of its own for its shingles and prefixes. No mapping defines them, so the
     * field mappings of an index report the search_as_you_type field and nothing below it.
     */
    public void testGetFieldMappingsDoesNotReportGeneratedFields() {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("title", "type=search_as_you_type", "other", "type=keyword"));

        Map<String, FieldMappingMetadata> all = indicesAdmin().prepareGetFieldMappings("test").setFields("*").get().mappings().get("test");
        assertThat(all.keySet(), hasItems("title", "other"));
        assertThat(all.keySet().stream().filter(field -> field.startsWith("title.")).toList(), empty());

        assertThat(indicesAdmin().prepareGetFieldMappings("test").setFields("title.*").get().mappings().get("test").keySet(), empty());
        for (String generated : new String[] { "title._2gram", "title._3gram", "title._index_prefix" }) {
            assertThat(
                generated,
                indicesAdmin().prepareGetFieldMappings("test").setFields(generated).get().fieldMappings("test", generated),
                nullValue()
            );
        }
    }
}
