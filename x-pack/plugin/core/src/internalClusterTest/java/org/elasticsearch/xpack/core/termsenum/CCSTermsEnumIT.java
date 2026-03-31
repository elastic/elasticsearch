/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumAction;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumRequest;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class CCSTermsEnumIT extends AbstractMultiClustersTestCase {

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of("remote_cluster");
    }

    @Override
    protected Settings nodeSettings() {
        // TODO Change this to run with security enabled
        // https://github.com/elastic/elasticsearch/issues/75940
        return Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(LocalStateCompositeXPackPlugin.class);
        return plugins;
    }

    public void testBasic() {
        Settings indexSettings = Settings.builder().put("index.number_of_replicas", 0).build();
        final Client localClient = client(LOCAL_CLUSTER);
        final Client remoteClient = client("remote_cluster");
        String localIndex = "local_test";
        assertAcked(localClient.admin().indices().prepareCreate(localIndex).setSettings(indexSettings));
        localClient.prepareIndex(localIndex).setSource("foo", "foo").get();
        localClient.prepareIndex(localIndex).setSource("foo", "foobar").get();
        localClient.admin().indices().prepareRefresh(localIndex).get();

        String remoteIndex = "remote_test";
        assertAcked(remoteClient.admin().indices().prepareCreate(remoteIndex).setSettings(indexSettings));
        remoteClient.prepareIndex(remoteIndex).setSource("foo", "bar").get();
        remoteClient.prepareIndex(remoteIndex).setSource("foo", "foobar").get();
        remoteClient.prepareIndex(remoteIndex).setSource("foo", "zar").get();
        remoteClient.admin().indices().prepareRefresh(remoteIndex).get();

        // _terms_enum on a remote cluster
        TermsEnumRequest req = new TermsEnumRequest("remote_cluster:remote_test").field("foo.keyword");
        TermsEnumResponse response = client().execute(TermsEnumAction.INSTANCE, req).actionGet();
        assertTrue(response.isComplete());
        assertThat(response.getTotalShards(), equalTo(1));
        assertThat(response.getSuccessfulShards(), equalTo(1));
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getTerms().size(), equalTo(3));
        assertThat(response.getTerms().get(0), equalTo("bar"));
        assertThat(response.getTerms().get(1), equalTo("foobar"));
        assertThat(response.getTerms().get(2), equalTo("zar"));

        // _terms_enum on mixed clusters (local + remote)
        req = new TermsEnumRequest("remote_cluster:remote_test", "local_test").field("foo.keyword");
        response = client().execute(TermsEnumAction.INSTANCE, req).actionGet();
        assertTrue(response.isComplete());
        assertThat(response.getTotalShards(), equalTo(2));
        assertThat(response.getSuccessfulShards(), equalTo(2));
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getTerms().size(), equalTo(4));
        assertThat(response.getTerms().get(0), equalTo("bar"));
        assertThat(response.getTerms().get(1), equalTo("foo"));
        assertThat(response.getTerms().get(2), equalTo("foobar"));
        assertThat(response.getTerms().get(3), equalTo("zar"));

        req = new TermsEnumRequest("remote_cluster:remote_test", "local_test").field("foo.keyword").searchAfter("foobar");
        response = client().execute(TermsEnumAction.INSTANCE, req).actionGet();
        assertTrue(response.isComplete());
        assertThat(response.getTotalShards(), equalTo(2));
        assertThat(response.getSuccessfulShards(), equalTo(2));
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getTerms().size(), equalTo(1));
        assertThat(response.getTerms().get(0), equalTo("zar"));

        req = new TermsEnumRequest("remote_cluster:remote_test", "local_test").field("foo.keyword").searchAfter("bar");
        response = client().execute(TermsEnumAction.INSTANCE, req).actionGet();
        assertTrue(response.isComplete());
        assertThat(response.getTotalShards(), equalTo(2));
        assertThat(response.getSuccessfulShards(), equalTo(2));
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getTerms().size(), equalTo(3));
        assertThat(response.getTerms().get(0), equalTo("foo"));
        assertThat(response.getTerms().get(1), equalTo("foobar"));
        assertThat(response.getTerms().get(2), equalTo("zar"));
    }
}
