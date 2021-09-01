/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

@ESIntegTestCase.SuiteScopeTestCase
public class IpRangeIT extends ESIntegTestCase {

    public static class DummyScriptPlugin extends MockScriptPlugin {
        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("dummy", params -> null);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(DummyScriptPlugin.class);
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("idx").addMapping("type", "ip", "type=ip", "ips", "type=ip"));
        waitForRelocation(ClusterHealthStatus.GREEN);

        indexRandom(
            true,
            client().prepareIndex("idx", "type", "1").setSource("ip", "192.168.1.7", "ips", Arrays.asList("192.168.0.13", "192.168.1.2")),
            client().prepareIndex("idx", "type", "2").setSource("ip", "192.168.1.10", "ips", Arrays.asList("192.168.1.25", "192.168.1.28")),
            client().prepareIndex("idx", "type", "3")
                .setSource("ip", "2001:db8::ff00:42:8329", "ips", Arrays.asList("2001:db8::ff00:42:8329", "2001:db8::ff00:42:8380"))
        );

        assertAcked(prepareCreate("idx_unmapped"));
        waitForRelocation(ClusterHealthStatus.GREEN);
        refresh();
    }

    public void testSingleValuedField() {
        SearchResponse rsp = client().prepareSearch("idx")
            .addAggregation(
                AggregationBuilders.ipRange("my_range")
                    .field("ip")
                    .addUnboundedTo("192.168.1.0")
                    .addRange("192.168.1.0", "192.168.1.10")
                    .addUnboundedFrom("192.168.1.10")
            )
            .get();
        assertSearchResponse(rsp);
        Range range = rsp.getAggregations().get("my_range");
        assertEquals(3, range.getBuckets().size());

        Range.Bucket bucket1 = range.getBuckets().get(0);
        assertNull(bucket1.getFrom());
        assertEquals("192.168.1.0", bucket1.getTo());
        assertEquals("*-192.168.1.0", bucket1.getKey());
        assertEquals(0, bucket1.getDocCount());

        Range.Bucket bucket2 = range.getBuckets().get(1);
        assertEquals("192.168.1.0", bucket2.getFrom());
        assertEquals("192.168.1.10", bucket2.getTo());
        assertEquals("192.168.1.0-192.168.1.10", bucket2.getKey());
        assertEquals(1, bucket2.getDocCount());

        Range.Bucket bucket3 = range.getBuckets().get(2);
        assertEquals("192.168.1.10", bucket3.getFrom());
        assertNull(bucket3.getTo());
        assertEquals("192.168.1.10-*", bucket3.getKey());
        assertEquals(2, bucket3.getDocCount());
    }

    public void testMultiValuedField() {
        SearchResponse rsp = client().prepareSearch("idx")
            .addAggregation(
                AggregationBuilders.ipRange("my_range")
                    .field("ips")
                    .addUnboundedTo("192.168.1.0")
                    .addRange("192.168.1.0", "192.168.1.10")
                    .addUnboundedFrom("192.168.1.10")
            )
            .get();
        assertSearchResponse(rsp);
        Range range = rsp.getAggregations().get("my_range");
        assertEquals(3, range.getBuckets().size());

        Range.Bucket bucket1 = range.getBuckets().get(0);
        assertNull(bucket1.getFrom());
        assertEquals("192.168.1.0", bucket1.getTo());
        assertEquals("*-192.168.1.0", bucket1.getKey());
        assertEquals(1, bucket1.getDocCount());

        Range.Bucket bucket2 = range.getBuckets().get(1);
        assertEquals("192.168.1.0", bucket2.getFrom());
        assertEquals("192.168.1.10", bucket2.getTo());
        assertEquals("192.168.1.0-192.168.1.10", bucket2.getKey());
        assertEquals(1, bucket2.getDocCount());

        Range.Bucket bucket3 = range.getBuckets().get(2);
        assertEquals("192.168.1.10", bucket3.getFrom());
        assertNull(bucket3.getTo());
        assertEquals("192.168.1.10-*", bucket3.getKey());
        assertEquals(2, bucket3.getDocCount());
    }

    public void testIpMask() {
        SearchResponse rsp = client().prepareSearch("idx")
            .addAggregation(
                AggregationBuilders.ipRange("my_range")
                    .field("ips")
                    .addMaskRange("::/0")
                    .addMaskRange("0.0.0.0/0")
                    .addMaskRange("2001:db8::/64")
            )
            .get();
        assertSearchResponse(rsp);
        Range range = rsp.getAggregations().get("my_range");
        assertEquals(3, range.getBuckets().size());

        Range.Bucket bucket1 = range.getBuckets().get(0);
        assertEquals("::/0", bucket1.getKey());
        assertEquals(3, bucket1.getDocCount());

        Range.Bucket bucket2 = range.getBuckets().get(1);
        assertEquals("0.0.0.0/0", bucket2.getKey());
        assertEquals(2, bucket2.getDocCount());

        Range.Bucket bucket3 = range.getBuckets().get(2);
        assertEquals("2001:db8::/64", bucket3.getKey());
        assertEquals(1, bucket3.getDocCount());
    }

    public void testPartiallyUnmapped() {
        SearchResponse rsp = client().prepareSearch("idx", "idx_unmapped")
            .addAggregation(
                AggregationBuilders.ipRange("my_range")
                    .field("ip")
                    .addUnboundedTo("192.168.1.0")
                    .addRange("192.168.1.0", "192.168.1.10")
                    .addUnboundedFrom("192.168.1.10")
            )
            .get();
        assertSearchResponse(rsp);
        Range range = rsp.getAggregations().get("my_range");
        assertEquals(3, range.getBuckets().size());

        Range.Bucket bucket1 = range.getBuckets().get(0);
        assertNull(bucket1.getFrom());
        assertEquals("192.168.1.0", bucket1.getTo());
        assertEquals("*-192.168.1.0", bucket1.getKey());
        assertEquals(0, bucket1.getDocCount());

        Range.Bucket bucket2 = range.getBuckets().get(1);
        assertEquals("192.168.1.0", bucket2.getFrom());
        assertEquals("192.168.1.10", bucket2.getTo());
        assertEquals("192.168.1.0-192.168.1.10", bucket2.getKey());
        assertEquals(1, bucket2.getDocCount());

        Range.Bucket bucket3 = range.getBuckets().get(2);
        assertEquals("192.168.1.10", bucket3.getFrom());
        assertNull(bucket3.getTo());
        assertEquals("192.168.1.10-*", bucket3.getKey());
        assertEquals(2, bucket3.getDocCount());
    }

    public void testUnmapped() {
        SearchResponse rsp = client().prepareSearch("idx_unmapped")
            .addAggregation(
                AggregationBuilders.ipRange("my_range")
                    .field("ip")
                    .addUnboundedTo("192.168.1.0")
                    .addRange("192.168.1.0", "192.168.1.10")
                    .addUnboundedFrom("192.168.1.10")
            )
            .get();
        assertSearchResponse(rsp);
        Range range = rsp.getAggregations().get("my_range");
        assertEquals(3, range.getBuckets().size());

        Range.Bucket bucket1 = range.getBuckets().get(0);
        assertNull(bucket1.getFrom());
        assertEquals("192.168.1.0", bucket1.getTo());
        assertEquals("*-192.168.1.0", bucket1.getKey());
        assertEquals(0, bucket1.getDocCount());

        Range.Bucket bucket2 = range.getBuckets().get(1);
        assertEquals("192.168.1.0", bucket2.getFrom());
        assertEquals("192.168.1.10", bucket2.getTo());
        assertEquals("192.168.1.0-192.168.1.10", bucket2.getKey());
        assertEquals(0, bucket2.getDocCount());

        Range.Bucket bucket3 = range.getBuckets().get(2);
        assertEquals("192.168.1.10", bucket3.getFrom());
        assertNull(bucket3.getTo());
        assertEquals("192.168.1.10-*", bucket3.getKey());
        assertEquals(0, bucket3.getDocCount());
    }

    public void testRejectsScript() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareSearch("idx")
                .addAggregation(
                    AggregationBuilders.ipRange("my_range")
                        .script(new Script(ScriptType.INLINE, "mockscript", "dummy", Collections.emptyMap()))
                )
                .get()
        );
        assertThat(e.getMessage(), containsString("[ip_range] does not support scripts"));
    }

    public void testRejectsValueScript() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareSearch("idx")
                .addAggregation(
                    AggregationBuilders.ipRange("my_range")
                        .field("ip")
                        .script(new Script(ScriptType.INLINE, "mockscript", "dummy", Collections.emptyMap()))
                )
                .get()
        );
        assertThat(e.getMessage(), containsString("[ip_range] does not support scripts"));
    }

    public void testNoRangesInQuery() {
        try {
            client().prepareSearch("idx").addAggregation(AggregationBuilders.ipRange("my_range").field("ip")).get();
            fail();
        } catch (SearchPhaseExecutionException spee) {
            Throwable rootCause = spee.getCause().getCause();
            assertThat(rootCause, instanceOf(IllegalArgumentException.class));
            assertEquals(rootCause.getMessage(), "No [ranges] specified for the [my_range] aggregation");
        }
    }
}
