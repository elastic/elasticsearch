package org.elasticsearch.xpack.enrich;

import java.util.List;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichFeatureSetUsage;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.CoordinatorStats;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.ExecutionStats;
import org.elasticsearch.xpack.enrich.action.EnrichStatsResponseTests;

public class EnrichFeatureSetUsageTests extends AbstractWireSerializingTestCase<EnrichFeatureSetUsage> {
    @Override
    protected EnrichFeatureSetUsage createTestInstance() {
        boolean available = randomBoolean();
        boolean enabled = randomBoolean();
        ExecutionStats executionStats = EnrichStatsResponseTests.randomExecutionStats();
        List<CoordinatorStats> coordinatorStats = EnrichStatsResponseTests.randomCoordinatorStats();
        return new EnrichFeatureSetUsage(available, enabled, executionStats, coordinatorStats);
    }

    @Override
    protected Writeable.Reader<EnrichFeatureSetUsage> instanceReader() {
        return EnrichFeatureSetUsage::new;
    }
}
