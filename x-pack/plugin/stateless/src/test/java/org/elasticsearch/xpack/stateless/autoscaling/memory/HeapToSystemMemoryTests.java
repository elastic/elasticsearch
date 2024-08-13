/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.memory;

import co.elastic.elasticsearch.serverless.constants.ProjectType;

import org.elasticsearch.test.ESTestCase;

import static co.elastic.elasticsearch.stateless.autoscaling.memory.HeapToSystemMemory.MAX_HEAP_SIZE;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.HeapToSystemMemory.VECTOR_HEAP_THRESHOLD;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.HeapToSystemMemory.dataNode;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.HeapToSystemMemory.tier;
import static org.hamcrest.CoreMatchers.equalTo;

public class HeapToSystemMemoryTests extends ESTestCase {
    public void testDefaultHeapToSystemMemory() {
        long heapInBytes = randomLongBetween(MemoryMetricsService.WORKLOAD_MEMORY_OVERHEAD, MAX_HEAP_SIZE);
        assertThat(dataNode(heapInBytes, ProjectType.ELASTICSEARCH_GENERAL_PURPOSE), equalTo(heapInBytes * 2));
        assertThat(tier(heapInBytes, ProjectType.ELASTICSEARCH_GENERAL_PURPOSE), equalTo(heapInBytes * 2));

        heapInBytes = randomLongBetween(MAX_HEAP_SIZE + 1, MAX_HEAP_SIZE * 10);
        assertThat(dataNode(heapInBytes, ProjectType.ELASTICSEARCH_GENERAL_PURPOSE), equalTo(MAX_HEAP_SIZE * 2));
        assertThat(tier(heapInBytes, ProjectType.ELASTICSEARCH_GENERAL_PURPOSE), equalTo(heapInBytes * 2));
    }

    public void testVectorHeapToSystemMemory() {
        long heapInBytes = randomLongBetween(MemoryMetricsService.WORKLOAD_MEMORY_OVERHEAD, VECTOR_HEAP_THRESHOLD);
        assertThat(dataNode(heapInBytes, ProjectType.ELASTICSEARCH_VECTOR), equalTo(heapInBytes * 2));
        assertThat(tier(heapInBytes, ProjectType.ELASTICSEARCH_VECTOR), equalTo(heapInBytes * 2));

        heapInBytes = randomLongBetween(VECTOR_HEAP_THRESHOLD, MAX_HEAP_SIZE);
        assertThat(dataNode(heapInBytes, ProjectType.ELASTICSEARCH_VECTOR), equalTo(heapInBytes * 4));
        assertThat(tier(heapInBytes, ProjectType.ELASTICSEARCH_VECTOR), equalTo(heapInBytes * 4));

        heapInBytes = randomLongBetween(MAX_HEAP_SIZE + 1, MAX_HEAP_SIZE * 10);
        assertThat(dataNode(heapInBytes, ProjectType.ELASTICSEARCH_VECTOR), equalTo(MAX_HEAP_SIZE * 4));
        assertThat(tier(heapInBytes, ProjectType.ELASTICSEARCH_VECTOR), equalTo(heapInBytes * 4));
    }
}
