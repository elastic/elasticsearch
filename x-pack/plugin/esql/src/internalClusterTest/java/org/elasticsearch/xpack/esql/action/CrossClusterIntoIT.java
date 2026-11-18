/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.xpack.esql.VerificationException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public class CrossClusterIntoIT extends AbstractCrossClusterTestCase {

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    /**
     * A remote that is unavailable and can be skipped must still make INTO reject the query, rather than write only the
     * local rows of a query that asked for more, and report success.
     */
    public void testUnavailableRemoteIsNotSilentlySkipped() throws Exception {
        assumeTrue("INTO is only enabled in snapshot builds", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
        setupClusters(2);
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate("dest-into"));
        cluster(REMOTE_CLUSTER_1).close();

        VerificationException e = expectThrows(
            VerificationException.class,
            () -> runQuery("FROM logs-*," + REMOTE_CLUSTER_1 + ":logs-* | KEEP v | INTO dest-into", null).close()
        );
        assertThat(e.getMessage(), containsString("INTO currently only supports ROW or local FROM as a source"));
    }
}
