/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.core.rollup.action.RollableIndexCaps;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.rollup.action.TransportGetRollupIndexCapsAction.getCapsByRollupIndex;
import static org.hamcrest.Matchers.equalTo;

public class GetRollupIndexCapsActionRequestTests extends AbstractWireSerializingTestCase<GetRollupIndexCapsAction.Request> {

    @Override
    protected GetRollupIndexCapsAction.Request createTestInstance() {
        if (randomBoolean()) {
            return new GetRollupIndexCapsAction.Request(new String[] { Metadata.ALL });
        }
        return new GetRollupIndexCapsAction.Request(new String[] { randomAlphaOfLengthBetween(1, 20) });
    }

    @Override
    protected Writeable.Reader<GetRollupIndexCapsAction.Request> instanceReader() {
        return GetRollupIndexCapsAction.Request::new;
    }

    public void testNoIndicesByRollup() {
        ImmutableOpenMap<String, IndexMetadata> indices = new ImmutableOpenMap.Builder<String, IndexMetadata>().build();
        Map<String, RollableIndexCaps> caps = getCapsByRollupIndex(Collections.singletonList("foo"), indices);
        assertThat(caps.size(), equalTo(0));
    }

    public void testAllIndicesByRollupSingleRollup() throws IOException {
        int num = randomIntBetween(1, 5);
        ImmutableOpenMap.Builder<String, IndexMetadata> indices = new ImmutableOpenMap.Builder<>(5);
        int indexCounter = 0;
        for (int j = 0; j < 5; j++) {

            Map<String, Object> jobs = new HashMap<>(num);
            for (int i = 0; i < num; i++) {
                String jobName = randomAlphaOfLength(10);
                String indexName = Integer.toString(indexCounter);
                indexCounter += 1;
                jobs.put(jobName, ConfigTestHelpers.randomRollupJobConfig(random(), jobName, indexName, "foo"));
            }

            MappingMetadata mappingMeta = new MappingMetadata(
                RollupField.TYPE_NAME,
                Collections.singletonMap(
                    RollupField.TYPE_NAME,
                    Collections.singletonMap("_meta", Collections.singletonMap(RollupField.ROLLUP_META, jobs))
                )
            );

            ImmutableOpenMap.Builder<String, MappingMetadata> mappings = ImmutableOpenMap.builder(1);
            mappings.put(RollupField.TYPE_NAME, mappingMeta);
            IndexMetadata meta = Mockito.mock(IndexMetadata.class);
            Mockito.when(meta.getMappings()).thenReturn(mappings.build());
            indices.put("foo", meta);
        }

        Map<String, RollableIndexCaps> caps = getCapsByRollupIndex(Collections.singletonList("foo"), indices.build());
        assertThat(caps.size(), equalTo(1));
    }

    public void testAllIndicesByRollupManyRollup() throws IOException {
        ImmutableOpenMap.Builder<String, IndexMetadata> indices = new ImmutableOpenMap.Builder<>(5);
        int indexCounter = 0;
        for (int j = 0; j < 5; j++) {

            Map<String, Object> jobs = new HashMap<>(1);
            String jobName = randomAlphaOfLength(10);
            String indexName = Integer.toString(indexCounter);
            indexCounter += 1;
            jobs.put(jobName, ConfigTestHelpers.randomRollupJobConfig(random(), jobName, indexName, "rollup_" + indexName));

            MappingMetadata mappingMeta = new MappingMetadata(
                RollupField.TYPE_NAME,
                Collections.singletonMap(
                    RollupField.TYPE_NAME,
                    Collections.singletonMap("_meta", Collections.singletonMap(RollupField.ROLLUP_META, jobs))
                )
            );

            ImmutableOpenMap.Builder<String, MappingMetadata> mappings = ImmutableOpenMap.builder(1);
            mappings.put(RollupField.TYPE_NAME, mappingMeta);
            IndexMetadata meta = Mockito.mock(IndexMetadata.class);
            Mockito.when(meta.getMappings()).thenReturn(mappings.build());
            indices.put("rollup_" + indexName, meta);
        }

        Map<String, RollableIndexCaps> caps = getCapsByRollupIndex(Arrays.asList(indices.keys().toArray(String.class)), indices.build());
        assertThat(caps.size(), equalTo(5));
    }

    public void testOneIndexByRollupManyRollup() throws IOException {
        ImmutableOpenMap.Builder<String, IndexMetadata> indices = new ImmutableOpenMap.Builder<>(5);
        int indexCounter = 0;
        for (int j = 0; j < 5; j++) {

            Map<String, Object> jobs = new HashMap<>(1);
            String jobName = randomAlphaOfLength(10);
            String indexName = Integer.toString(indexCounter);
            indexCounter += 1;
            jobs.put(jobName, ConfigTestHelpers.randomRollupJobConfig(random(), jobName, "foo_" + indexName, "rollup_" + indexName));

            MappingMetadata mappingMeta = new MappingMetadata(
                RollupField.TYPE_NAME,
                Collections.singletonMap(
                    RollupField.TYPE_NAME,
                    Collections.singletonMap("_meta", Collections.singletonMap(RollupField.ROLLUP_META, jobs))
                )
            );

            ImmutableOpenMap.Builder<String, MappingMetadata> mappings = ImmutableOpenMap.builder(1);
            mappings.put(RollupField.TYPE_NAME, mappingMeta);
            IndexMetadata meta = Mockito.mock(IndexMetadata.class);
            Mockito.when(meta.getMappings()).thenReturn(mappings.build());
            indices.put("rollup_" + indexName, meta);
        }

        Map<String, RollableIndexCaps> caps = getCapsByRollupIndex(Collections.singletonList("rollup_1"), indices.build());
        assertThat(caps.size(), equalTo(1));
        assertThat(caps.get("rollup_1").getIndexName(), equalTo("rollup_1"));
        assertThat(caps.get("rollup_1").getJobCaps().size(), equalTo(1));
    }

    public void testOneIndexByRollupOneRollup() throws IOException {
        ImmutableOpenMap.Builder<String, IndexMetadata> indices = new ImmutableOpenMap.Builder<>(5);
        int indexCounter = 0;
        for (int j = 0; j < 5; j++) {

            Map<String, Object> jobs = new HashMap<>(1);
            String jobName = randomAlphaOfLength(10);
            String indexName = Integer.toString(indexCounter);
            indexCounter += 1;
            jobs.put(jobName, ConfigTestHelpers.randomRollupJobConfig(random(), jobName, "foo_" + indexName, "rollup_foo"));

            MappingMetadata mappingMeta = new MappingMetadata(
                RollupField.TYPE_NAME,
                Collections.singletonMap(
                    RollupField.TYPE_NAME,
                    Collections.singletonMap("_meta", Collections.singletonMap(RollupField.ROLLUP_META, jobs))
                )
            );

            ImmutableOpenMap.Builder<String, MappingMetadata> mappings = ImmutableOpenMap.builder(1);
            mappings.put(RollupField.TYPE_NAME, mappingMeta);
            IndexMetadata meta = Mockito.mock(IndexMetadata.class);
            Mockito.when(meta.getMappings()).thenReturn(mappings.build());
            indices.put("rollup_foo", meta);
        }

        Map<String, RollableIndexCaps> caps = getCapsByRollupIndex(Collections.singletonList("rollup_foo"), indices.build());
        assertThat(caps.size(), equalTo(1));
        assertThat(caps.get("rollup_foo").getIndexName(), equalTo("rollup_foo"));
        assertThat(caps.get("rollup_foo").getJobCaps().size(), equalTo(1));
    }
}
