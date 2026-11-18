/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlStatsAction;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class EsqlUsageTransportActionTests extends ESTestCase {

    /**
     * Phone-home usage keeps the counters of the fetch phase and leaves out the fetch contexts open on the nodes, which are
     * a live gauge of {@code GET _esql/stats}.
     */
    public void testUsageLeavesOutTheOpenFetchContexts() {
        Counters counters = new Counters();
        counters.inc("fetch_phase.applied", 3);
        counters.inc(TransportEsqlStatsAction.FETCH_PHASE_OPEN_CONTEXTS, 2);

        Map<String, Object> usage = EsqlUsageTransportAction.usageMapWithoutGauges(counters);
        assertThat(usage.get("fetch_phase"), equalTo(Map.of("applied", 3L)));
    }

    /**
     * Without other counters of the fetch phase, usage has no {@code fetch_phase} section.
     */
    public void testUsageWithOnlyTheOpenFetchContextsHasNoFetchPhaseSection() {
        Counters counters = new Counters();
        counters.inc("queries.rest.total", 1);
        counters.inc(TransportEsqlStatsAction.FETCH_PHASE_OPEN_CONTEXTS, 2);

        assertThat(EsqlUsageTransportAction.usageMapWithoutGauges(counters), not(hasKey("fetch_phase")));
    }

    public void testUsageLeavesOutTheAdmissionGauges() {
        Counters counters = new Counters();
        counters.inc("datasources.admission.bytes.used", 10);
        counters.inc("datasources.discovery.files_scanned.1", 1);

        Map<?, ?> datasources = (Map<?, ?>) EsqlUsageTransportAction.usageMapWithoutGauges(counters).get("datasources");
        assertThat(datasources, not(hasKey("admission")));
        assertThat(datasources, hasKey("discovery"));
    }
}
