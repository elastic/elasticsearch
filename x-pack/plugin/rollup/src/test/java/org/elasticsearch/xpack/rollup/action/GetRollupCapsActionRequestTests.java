/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;


import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.GetRollupCapsAction;
import org.elasticsearch.xpack.core.rollup.action.RollableIndexCaps;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;


public class GetRollupCapsActionRequestTests extends AbstractWireSerializingTestCase<GetRollupCapsAction.Request> {

    @Override
    protected GetRollupCapsAction.Request createTestInstance() {
        if (randomBoolean()) {
            return new GetRollupCapsAction.Request(MetaData.ALL);
        }
        return new GetRollupCapsAction.Request(randomAlphaOfLengthBetween(1, 20));
    }

    @Override
    protected Writeable.Reader<GetRollupCapsAction.Request> instanceReader() {
        return GetRollupCapsAction.Request::new;
    }

    public void testNoIndexMetaData() {
        String indexPattern = randomBoolean() ? randomAlphaOfLength(10) : randomAlphaOfLength(10) + "-*";
        Optional<RollupIndexCaps> caps = TransportGetRollupCapsAction.findRollupIndexCaps(indexPattern, null);
        assertFalse(caps.isPresent());
    }

    public void testMissingRollup() {
        String indexPattern = randomBoolean() ? randomAlphaOfLength(10) : randomAlphaOfLength(10) + "-*";

        ImmutableOpenMap<String, MappingMetaData> mappings = ImmutableOpenMap.of();
        IndexMetaData meta = Mockito.mock(IndexMetaData.class);
        Mockito.when(meta.getMappings()).thenReturn(mappings);
        Optional<RollupIndexCaps> caps = TransportGetRollupCapsAction.findRollupIndexCaps(indexPattern, meta);
        assertFalse(caps.isPresent());
    }

    public void testMissingMeta() throws IOException {
        String indexPattern = randomBoolean() ? randomAlphaOfLength(10) : randomAlphaOfLength(10) + "-*";

        MappingMetaData mappingMeta = new MappingMetaData(RollupField.NAME, Collections.emptyMap());

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder(1);
        mappings.put(RollupField.NAME, mappingMeta);
        IndexMetaData meta = Mockito.mock(IndexMetaData.class);
        Mockito.when(meta.getMappings()).thenReturn(mappings.build());
        Optional<RollupIndexCaps> caps = TransportGetRollupCapsAction.findRollupIndexCaps(indexPattern, meta);
        assertFalse(caps.isPresent());
    }

    public void testMissingJob() throws IOException {
        String indexPattern = randomBoolean() ? randomAlphaOfLength(10) : randomAlphaOfLength(10) + "-*";

        MappingMetaData mappingMeta = new MappingMetaData(RollupField.NAME, Collections.singletonMap(RollupField.NAME,
            Collections.singletonMap("_meta",
                Collections.emptyMap())));

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder(1);
        mappings.put(RollupField.NAME, mappingMeta);
        IndexMetaData meta = Mockito.mock(IndexMetaData.class);
        Mockito.when(meta.getMappings()).thenReturn(mappings.build());
        Optional<RollupIndexCaps> caps = TransportGetRollupCapsAction.findRollupIndexCaps(indexPattern, meta);
        assertFalse(caps.isPresent());
    }

    public void testOneJob() throws IOException {
        String indexPattern = randomBoolean() ? randomAlphaOfLength(10) : randomAlphaOfLength(10) + "-*";
        String jobName = randomAlphaOfLength(5);
        RollupJobConfig job = ConfigTestHelpers.randomRollupJobConfig(random(), jobName);

        MappingMetaData mappingMeta = new MappingMetaData(RollupField.TYPE_NAME,
            Collections.singletonMap(RollupField.TYPE_NAME,
                Collections.singletonMap("_meta",
                    Collections.singletonMap(RollupField.ROLLUP_META,
                        Collections.singletonMap(jobName, job)))));

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder(1);
        mappings.put(RollupField.TYPE_NAME, mappingMeta);
        IndexMetaData meta = Mockito.mock(IndexMetaData.class);
        Mockito.when(meta.getMappings()).thenReturn(mappings.build());
        Optional<RollupIndexCaps> caps = TransportGetRollupCapsAction.findRollupIndexCaps(indexPattern, meta);
        assertTrue(caps.isPresent());
        assertThat(caps.get().getJobCaps().size(), equalTo(1));
    }

    public void testMultipleJobs() throws IOException {
        String indexPattern = randomBoolean() ? randomAlphaOfLength(10) : randomAlphaOfLength(10) + "-*";

        int num = randomIntBetween(1,5);
        Map<String, Object> jobs = new HashMap<>(num);
        for (int i = 0; i < num; i++) {
            String jobName = randomAlphaOfLength(5);
            jobs.put(jobName, ConfigTestHelpers.randomRollupJobConfig(random(), jobName));
        }

        MappingMetaData mappingMeta = new MappingMetaData(RollupField.TYPE_NAME,
            Collections.singletonMap(RollupField.TYPE_NAME,
                Collections.singletonMap("_meta",
                    Collections.singletonMap(RollupField.ROLLUP_META, jobs))));

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder(1);
        mappings.put(RollupField.TYPE_NAME, mappingMeta);
        IndexMetaData meta = Mockito.mock(IndexMetaData.class);
        Mockito.when(meta.getMappings()).thenReturn(mappings.build());
        Optional<RollupIndexCaps> caps = TransportGetRollupCapsAction.findRollupIndexCaps(indexPattern, meta);
        assertTrue(caps.isPresent());
        assertThat(caps.get().getJobCaps().size(), equalTo(num));
    }

    public void testNoIndices() {
        ImmutableOpenMap<String, IndexMetaData> indices = new ImmutableOpenMap.Builder<String, IndexMetaData>().build();
        Map<String, RollableIndexCaps> caps = TransportGetRollupCapsAction.getCaps("foo", indices);
        assertThat(caps.size(), equalTo(0));
    }

    public void testAllIndices() throws IOException {
        int num = randomIntBetween(1,5);
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = new ImmutableOpenMap.Builder<>(5);
        int indexCounter = 0;
        for (int j = 0; j < 5; j++) {

            Map<String, Object> jobs = new HashMap<>(num);
            for (int i = 0; i < num; i++) {
                String jobName = randomAlphaOfLength(10);
                String indexName = Integer.toString(indexCounter);
                indexCounter += 1;
                jobs.put(jobName, ConfigTestHelpers.randomRollupJobConfig(random(), jobName, indexName));
            }

            MappingMetaData mappingMeta = new MappingMetaData(RollupField.TYPE_NAME,
                Collections.singletonMap(RollupField.TYPE_NAME,
                    Collections.singletonMap("_meta",
                        Collections.singletonMap(RollupField.ROLLUP_META, jobs))));

            ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder(1);
            mappings.put(RollupField.TYPE_NAME, mappingMeta);
            IndexMetaData meta = Mockito.mock(IndexMetaData.class);
            Mockito.when(meta.getMappings()).thenReturn(mappings.build());
            indices.put(randomAlphaOfLength(10), meta);
        }

        Map<String, RollableIndexCaps> caps = TransportGetRollupCapsAction.getCaps(MetaData.ALL, indices.build());
        assertThat(caps.size(), equalTo(num * 5));
    }

    public void testOneIndex() throws IOException {
        int num = randomIntBetween(1,5);
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = new ImmutableOpenMap.Builder<>(5);
        String selectedIndexName = null;
        for (int j = 0; j < 5; j++) {
            String indexName = randomAlphaOfLength(10);
            if (selectedIndexName == null) {
                selectedIndexName = indexName;
            }

            Map<String, Object> jobs = new HashMap<>(num);
            for (int i = 0; i < num; i++) {
                String jobName = randomAlphaOfLength(5);
                jobs.put(jobName, ConfigTestHelpers.randomRollupJobConfig(random(), jobName, indexName));
            }

            MappingMetaData mappingMeta = new MappingMetaData(RollupField.TYPE_NAME,
                Collections.singletonMap(RollupField.TYPE_NAME,
                    Collections.singletonMap("_meta",
                        Collections.singletonMap(RollupField.ROLLUP_META, jobs))));

            ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder(1);
            mappings.put(RollupField.TYPE_NAME, mappingMeta);
            IndexMetaData meta = Mockito.mock(IndexMetaData.class);
            Mockito.when(meta.getMappings()).thenReturn(mappings.build());

            indices.put(indexName, meta);
        }

        Map<String, RollableIndexCaps> caps = TransportGetRollupCapsAction.getCaps(selectedIndexName, indices.build());
        assertThat(caps.size(), equalTo(1));
    }

    public void testNonRollupMeta() throws IOException {
        String indexPattern = randomBoolean() ? randomAlphaOfLength(10) : randomAlphaOfLength(10) + "-*";

        MappingMetaData mappingMeta = new MappingMetaData(RollupField.TYPE_NAME,
            Collections.singletonMap(RollupField.TYPE_NAME,
                Collections.singletonMap("_meta",
                    Collections.singletonMap("foo",
                        Collections.singletonMap("bar", "baz")))));

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder(1);
        mappings.put(RollupField.TYPE_NAME, mappingMeta);
        IndexMetaData meta = Mockito.mock(IndexMetaData.class);
        Mockito.when(meta.getMappings()).thenReturn(mappings.build());
        Optional<RollupIndexCaps> caps = TransportGetRollupCapsAction.findRollupIndexCaps(indexPattern, meta);
        assertFalse(caps.isPresent());
    }

    public void testNonRollupPlusRollupMeta() throws IOException {
        String indexPattern = randomBoolean() ? randomAlphaOfLength(10) : randomAlphaOfLength(10) + "-*";
        String jobName = randomAlphaOfLength(5);
        RollupJobConfig job = ConfigTestHelpers.randomRollupJobConfig(random(), jobName);

        Map<String, Object> metaMap = new HashMap<>(2);
        metaMap.put("foo", Collections.singletonMap("bar", "baz"));
        metaMap.put(RollupField.ROLLUP_META, Collections.singletonMap(jobName, job));

        MappingMetaData mappingMeta = new MappingMetaData(RollupField.TYPE_NAME,
            Collections.singletonMap(RollupField.TYPE_NAME,
                Collections.singletonMap("_meta", metaMap)));

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder(1);
        mappings.put(RollupField.TYPE_NAME, mappingMeta);
        IndexMetaData meta = Mockito.mock(IndexMetaData.class);
        Mockito.when(meta.getMappings()).thenReturn(mappings.build());
        Optional<RollupIndexCaps> caps = TransportGetRollupCapsAction.findRollupIndexCaps(indexPattern, meta);
        assertTrue(caps.isPresent());
        assertThat(caps.get().getJobCaps().size(), equalTo(1));
        assertThat(caps.get().getJobCaps().get(0).getJobID(), equalTo(jobName));
    }

    public void testRandomNonRollupPlusRollupMeta() throws IOException {
        String indexPattern = randomBoolean() ? randomAlphaOfLength(10) : randomAlphaOfLength(10) + "-*";

        Map<String, Object> metaMap = new HashMap<>();
        int numUnrelated = randomIntBetween(0, 10);
        for (int i = 0; i < numUnrelated; i++) {
            int numFields = randomIntBetween(0, 5);
            Map<String, Object> fields = new HashMap<>(numFields);
            for (int j = 0; j < numFields; j++) {
                int numFields2 = randomIntBetween(0, 2);
                Map<String, String> fields2 = new HashMap<>(numFields2);
                for (int k = 0; k < numFields; k++) {
                    fields2.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
                }
                fields.put(randomAlphaOfLength(5), fields2);
            }
            metaMap.put(randomAlphaOfLength(5), fields);
        }

        int numJobs = randomIntBetween(1,5);
        Map<String, Object> jobs = new HashMap<>(numJobs);
        for (int i = 0; i < numJobs; i++) {
            String name = randomAlphaOfLength(5);
            jobs.put(name, ConfigTestHelpers.randomRollupJobConfig(random(), name));
        }
        metaMap.put(RollupField.ROLLUP_META, jobs);

        MappingMetaData mappingMeta = new MappingMetaData(RollupField.TYPE_NAME,
            Collections.singletonMap(RollupField.TYPE_NAME,
                Collections.singletonMap("_meta", metaMap)));

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder(1);
        mappings.put(RollupField.TYPE_NAME, mappingMeta);
        IndexMetaData meta = Mockito.mock(IndexMetaData.class);
        Mockito.when(meta.getMappings()).thenReturn(mappings.build());
        Optional<RollupIndexCaps> caps = TransportGetRollupCapsAction.findRollupIndexCaps(indexPattern, meta);
        assertTrue(caps.isPresent());
        assertThat(caps.get().getJobCaps().size(), equalTo(numJobs));
    }

    public void testEmptyType() throws IOException {
        String indexPattern = randomBoolean() ? randomAlphaOfLength(10) : randomAlphaOfLength(10) + "-*";

        MappingMetaData mappingMeta = new MappingMetaData(RollupField.TYPE_NAME,
            Collections.singletonMap(RollupField.TYPE_NAME, Collections.emptyMap()));

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder(1);
        mappings.put(RollupField.TYPE_NAME, mappingMeta);
        IndexMetaData meta = Mockito.mock(IndexMetaData.class);
        Mockito.when(meta.getMappings()).thenReturn(mappings.build());
        Optional<RollupIndexCaps> caps = TransportGetRollupCapsAction.findRollupIndexCaps(indexPattern, meta);
        assertFalse(caps.isPresent());
    }
}


