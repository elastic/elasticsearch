/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos.archiveindex;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.oldrepos.AbstractUpgradeCompatibilityTestCase;
import org.elasticsearch.test.cluster.util.Version;

import static org.elasticsearch.test.rest.ObjectPath.createFromResponse;

/**
 * Test suite for Archive indices backward compatibility with N-2 versions.
 * The test suite creates a cluster in the N-1 version, where N is the current version.
 * Restores snapshots from old-clusters (version 5/6) and upgrades it to the current version.
 * Test methods are executed after each upgrade.
 *
 * For example the test suite creates a cluster of version 8, then restores a snapshot of an index created
 * when deployed ES version 5/6. The cluster then upgrades to version 9, verifying that the archive index
 * is successfully restored.
 */
public class ArchiveIndexTestCase extends AbstractUpgradeCompatibilityTestCase {

    static {
        clusterConfig = config -> config.setting("xpack.license.self_generated.type", "trial");
    }

    public ArchiveIndexTestCase(Version version) {
        super(version);
    }

    /**
     * Overrides the snapshot-restore operation for archive-indices scenario.
     */
    @Override
    public void recover(RestClient client, String repository, String snapshot, String index) throws Exception {
        var request = new Request("POST", "/_snapshot/" + repository + "/" + snapshot + "/_restore");
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity(Strings.format("""
            {
              "indices": "%s",
              "include_global_state": false,
              "rename_pattern": "(.+)",
              "include_aliases": false
            }""", index));
        createFromResponse(client.performRequest(request));
    }
}
