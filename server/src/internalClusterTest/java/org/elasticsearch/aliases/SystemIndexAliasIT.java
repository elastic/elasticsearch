/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aliases;

import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.indices.TestSystemIndexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.TestSystemIndexDescriptor.INDEX_NAME;
import static org.elasticsearch.indices.TestSystemIndexDescriptor.PRIMARY_INDEX_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SystemIndexAliasIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestSystemIndexPlugin.class);
    }

    public void testCreateAliasForSystemIndex() throws Exception {
        createIndex(PRIMARY_INDEX_NAME);
        ensureGreen();
        assertAcked(admin().indices().prepareAliases().addAlias(PRIMARY_INDEX_NAME, INDEX_NAME + "-system-alias"));

        final GetAliasesResponse getAliasesResponse = indicesAdmin().getAliases(
            new GetAliasesRequest().indicesOptions(IndicesOptions.strictExpandHidden())
        ).get();

        assertThat(getAliasesResponse.getAliases().size(), equalTo(1));
        assertThat(getAliasesResponse.getAliases().get(PRIMARY_INDEX_NAME).size(), equalTo(2));
        assertThat(
            getAliasesResponse.getAliases().get(PRIMARY_INDEX_NAME).stream().map(AliasMetadata::alias).collect(Collectors.toSet()),
            containsInAnyOrder(".test-index", ".test-index-system-alias")
        );
        getAliasesResponse.getAliases().get(PRIMARY_INDEX_NAME).forEach(alias -> assertThat(alias.isHidden(), is(true)));

        assertAcked(client().admin().indices().prepareDeleteTemplate("*").get());
    }
}
