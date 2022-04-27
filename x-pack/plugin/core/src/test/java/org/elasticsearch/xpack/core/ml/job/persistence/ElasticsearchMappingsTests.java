/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.persistence;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.core.ml.job.results.ReservedFieldNames;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.mockito.ArgumentCaptor;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ElasticsearchMappingsTests extends ESTestCase {

    // These are not reserved because they're Elasticsearch keywords, not
    // field names
    private static final List<String> KEYWORDS = Arrays.asList(
        ElasticsearchMappings.ANALYZER,
        ElasticsearchMappings.COPY_TO,
        ElasticsearchMappings.DYNAMIC,
        ElasticsearchMappings.ENABLED,
        ElasticsearchMappings.NESTED,
        ElasticsearchMappings.PROPERTIES,
        ElasticsearchMappings.TYPE,
        ElasticsearchMappings.WHITESPACE,
        SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD.getPreferredName()
    );

    private static final List<String> INTERNAL_FIELDS = Arrays.asList(GetResult._ID, GetResult._INDEX);

    public void testResultsMappingReservedFields() throws Exception {
        Set<String> overridden = new HashSet<>(KEYWORDS);

        // These are not reserved because they're data types, not field names
        overridden.add(Result.TYPE.getPreferredName());
        overridden.add(DataCounts.TYPE.getPreferredName());
        overridden.add(CategoryDefinition.TYPE.getPreferredName());
        overridden.add(ModelSizeStats.RESULT_TYPE_FIELD.getPreferredName());
        overridden.add(ModelSnapshot.TYPE.getPreferredName());
        overridden.add(Quantiles.TYPE.getPreferredName());
        overridden.add(TimingStats.TYPE.getPreferredName());
        overridden.add(DatafeedTimingStats.TYPE.getPreferredName());
        // This is a special case so that categorical job results can be paired easily with anomaly results
        // This is acceptable as both mappings are keyword for the results documents and for category definitions
        overridden.add(CategoryDefinition.MLCATEGORY.getPreferredName());

        Set<String> expected = collectResultsDocFieldNames();
        expected.removeAll(overridden);
        expected.addAll(INTERNAL_FIELDS);

        compareFields(expected, ReservedFieldNames.RESERVED_RESULT_FIELD_NAMES);
    }

    private void compareFields(Set<String> expected, Set<String> reserved) {
        if (reserved.size() != expected.size()) {
            Set<String> diff = new HashSet<>(reserved);
            diff.removeAll(expected);
            StringBuilder errorMessage = new StringBuilder("Fields in ReservedFieldNames but not in expected: ").append(diff);

            diff = new HashSet<>(expected);
            diff.removeAll(reserved);
            errorMessage.append("\nFields in expected but not in ReservedFieldNames: ").append(diff);
            fail(errorMessage.toString());
        }
        assertEquals(reserved.size(), expected.size());

        for (String s : expected) {
            // By comparing like this the failure messages say which string is missing
            String reservedField = reserved.contains(s) ? s : null;
            assertEquals(s, reservedField);
        }
    }

    public void testMappingRequiresUpdateNoMapping() {
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        ClusterState cs = csBuilder.build();
        String[] indices = new String[] { "no_index" };

        assertArrayEquals(new String[] { "no_index" }, ElasticsearchMappings.mappingRequiresUpdate(cs, indices, Version.CURRENT));
    }

    public void testMappingRequiresUpdateNullMapping() {
        ClusterState cs = getClusterStateWithMappingsWithMetadata(Collections.singletonMap("null_mapping", null));
        String[] indices = new String[] { "null_index" };
        assertArrayEquals(indices, ElasticsearchMappings.mappingRequiresUpdate(cs, indices, Version.CURRENT));
    }

    public void testMappingRequiresUpdateNoVersion() {
        ClusterState cs = getClusterStateWithMappingsWithMetadata(Collections.singletonMap("no_version_field", "NO_VERSION_FIELD"));
        String[] indices = new String[] { "no_version_field" };
        assertArrayEquals(indices, ElasticsearchMappings.mappingRequiresUpdate(cs, indices, Version.CURRENT));
    }

    public void testMappingRequiresUpdateRecentMappingVersion() {
        ClusterState cs = getClusterStateWithMappingsWithMetadata(Collections.singletonMap("version_current", Version.CURRENT.toString()));
        String[] indices = new String[] { "version_current" };
        assertArrayEquals(new String[] {}, ElasticsearchMappings.mappingRequiresUpdate(cs, indices, Version.CURRENT));
    }

    public void testMappingRequiresUpdateMaliciousMappingVersion() {
        ClusterState cs = getClusterStateWithMappingsWithMetadata(
            Collections.singletonMap("version_current", Collections.singletonMap("nested", "1.0"))
        );
        String[] indices = new String[] { "version_nested" };
        assertArrayEquals(indices, ElasticsearchMappings.mappingRequiresUpdate(cs, indices, Version.CURRENT));
    }

    public void testMappingRequiresUpdateBogusMappingVersion() {
        ClusterState cs = getClusterStateWithMappingsWithMetadata(Collections.singletonMap("version_bogus", "0.0"));
        String[] indices = new String[] { "version_bogus" };
        assertArrayEquals(indices, ElasticsearchMappings.mappingRequiresUpdate(cs, indices, Version.CURRENT));
    }

    public void testMappingRequiresUpdateNewerMappingVersion() {
        ClusterState cs = getClusterStateWithMappingsWithMetadata(Collections.singletonMap("version_newer", Version.CURRENT));
        String[] indices = new String[] { "version_newer" };
        assertArrayEquals(new String[] {}, ElasticsearchMappings.mappingRequiresUpdate(cs, indices, VersionUtils.getPreviousVersion()));
    }

    public void testMappingRequiresUpdateNewerMappingVersionMinor() {
        ClusterState cs = getClusterStateWithMappingsWithMetadata(Collections.singletonMap("version_newer_minor", Version.CURRENT));
        String[] indices = new String[] { "version_newer_minor" };
        assertArrayEquals(
            new String[] {},
            ElasticsearchMappings.mappingRequiresUpdate(cs, indices, VersionUtils.getPreviousMinorVersion())
        );
    }

    @SuppressWarnings({ "unchecked" })
    public void testAddDocMappingIfMissing() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(invocationOnMock -> {
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocationOnMock.getArguments()[2];
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(client).execute(eq(PutMappingAction.INSTANCE), any(), any(ActionListener.class));

        ClusterState clusterState = getClusterStateWithMappingsWithMetadata(Collections.singletonMap("index-name", "0.0"));
        ElasticsearchMappings.addDocMappingIfMissing(
            "index-name",
            () -> """
                {"_doc":{"properties":{"some-field":{"type":"long"}}}}""",
            client,
            clusterState,
            MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT,
            ActionListener.wrap(ok -> assertTrue(ok), e -> fail(e.toString()))
        );

        ArgumentCaptor<PutMappingRequest> requestCaptor = ArgumentCaptor.forClass(PutMappingRequest.class);
        verify(client).threadPool();
        verify(client).execute(eq(PutMappingAction.INSTANCE), requestCaptor.capture(), any(ActionListener.class));
        verifyNoMoreInteractions(client);

        PutMappingRequest request = requestCaptor.getValue();
        assertThat(request.indices(), equalTo(new String[] { "index-name" }));
        assertThat(request.source(), equalTo("""
            {"_doc":{"properties":{"some-field":{"type":"long"}}}}"""));
    }

    private ClusterState getClusterStateWithMappingsWithMetadata(Map<String, Object> namesAndVersions) {
        Metadata.Builder metadataBuilder = Metadata.builder();

        for (Map.Entry<String, Object> entry : namesAndVersions.entrySet()) {

            String indexName = entry.getKey();
            Object version = entry.getValue();

            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
            indexMetadata.settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            );

            Map<String, Object> mapping = new HashMap<>();
            Map<String, Object> properties = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                properties.put("field" + i, Collections.singletonMap("type", "string"));
            }
            mapping.put("properties", properties);

            Map<String, Object> meta = new HashMap<>();
            if (version != null && version.equals("NO_VERSION_FIELD") == false) {
                meta.put("version", version);
            }
            mapping.put("_meta", meta);

            indexMetadata.putMapping(new MappingMetadata("_doc", mapping));

            metadataBuilder.put(indexMetadata);
        }
        Metadata metadata = metadataBuilder.build();

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.metadata(metadata);
        return csBuilder.build();
    }

    private Set<String> collectResultsDocFieldNames() throws IOException {
        // Only the mappings for the results index should be added below. Do NOT add mappings for other indexes here.
        return collectFieldNames(AnomalyDetectorsIndex.resultsMapping());
    }

    private Set<String> collectFieldNames(String mapping) throws IOException {
        BufferedInputStream inputStream = new BufferedInputStream(new ByteArrayInputStream(mapping.getBytes(StandardCharsets.UTF_8)));
        XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, inputStream);
        Set<String> fieldNames = new HashSet<>();
        boolean isAfterPropertiesStart = false;
        try {
            XContentParser.Token token = parser.nextToken();
            while (token != null) {
                switch (token) {
                    case START_OBJECT:
                        break;
                    case FIELD_NAME:
                        String fieldName = parser.currentName();
                        if (isAfterPropertiesStart) {
                            fieldNames.add(fieldName);
                        } else {
                            if (ElasticsearchMappings.PROPERTIES.equals(fieldName)) {
                                isAfterPropertiesStart = true;
                            }
                        }
                        break;
                    default:
                        break;
                }
                token = parser.nextToken();
            }
        } catch (XContentParseException e) {
            fail("Cannot parse JSON: " + e);
        }

        return fieldNames;
    }
}
