/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.util.Version;

import java.util.List;
import java.util.function.Consumer;

public class SearchableSnapshotIT extends AbstractUpgradeCompatibilityTestCase {

    static {
        clusterConfig = config -> config.setting("xpack.license.self_generated.type", "trial");
    }

    public SearchableSnapshotIT(Version version) {
        super(version);
    }

    /**
     * Test case restoring a snapshot created in ES_v6 - Basic mapping
     * 1. Index Created in ES_v6
     * 2. Added 1 documents to index and created a snapshot (Steps 1-2 into resources/snapshot_v6.zip)
     * 3. Index Restored to version: Current-1: 8.x
     * 4. Cluster upgraded to version Current: 9.x
     */
    public void testRestoreArchiveIndexVersion6() throws Exception {
        verifyCompatibility(TestSnapshotCases.ES_VERSION_6);
    }

    /**
     * Test case mounting a snapshot created in ES_v5 - Basic mapping
     * 1. Index Created in ES_v5
     * 2. Added 1 documents to index and created a snapshot (Steps 1-2 into resources/snapshot_v5.zip)
     * 3. Index Restored to version: Current-1: 8.x
     * 4. Cluster upgraded to version Current: 9.x
     */
    public void testMountSearchableSnapshotVersion5() throws Exception {
        verifyCompatibility(TestSnapshotCases.ES_VERSION_5);
    }

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
