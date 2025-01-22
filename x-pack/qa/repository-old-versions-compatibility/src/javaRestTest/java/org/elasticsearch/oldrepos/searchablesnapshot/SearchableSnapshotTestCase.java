/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos.searchablesnapshot;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.oldrepos.AbstractUpgradeCompatibilityTestCase;
import org.elasticsearch.test.cluster.util.Version;

import java.util.List;
import java.util.function.Consumer;

/**
 * Test suite for Searchable indices backward compatibility with N-2 versions.
 * The test suite creates a cluster in the N-1 version, where N is the current version.
 * Restores snapshots from old-clusters (version 5/6) and upgrades it to the current version.
 * Test methods are executed after each upgrade.
 */
abstract class SearchableSnapshotTestCase extends AbstractUpgradeCompatibilityTestCase {

    static {
        clusterConfig = config -> config.setting("xpack.license.self_generated.type", "trial");
    }

    protected SearchableSnapshotTestCase(Version version, String indexCreatedVersion) {
        this(version, indexCreatedVersion, o -> {});
    }

    protected SearchableSnapshotTestCase(Version version, String indexCreatedVersion, Consumer<List<String>> warningsConsumer) {
        super(version, indexCreatedVersion, warningsConsumer);
    }

    /**
     * Overrides the snapshot-restore operation for archive-indices scenario.
     */
    @Override
    public void recover(RestClient client, String repository, String snapshot, String index, Consumer<List<String>> warningsConsumer)
        throws Exception {
        var request = new Request("POST", "/_snapshot/" + repository + "/" + snapshot + "/_mount");
        request.addParameter("wait_for_completion", "true");
        request.addParameter("storage", "full_copy");
        request.setJsonEntity(Strings.format("""
             {
              "index": "%s",
              "renamed_index": "%s"
            }""", index, index));
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        Response response = client.performRequest(request);
        assertOK(response);
        warningsConsumer.accept(response.getWarnings());
    }
}
