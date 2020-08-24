/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryShardContextTests extends ESTestCase {

    public void testFailIfFieldMappingNotFound() {
        QueryShardContext context = createQueryShardContext(IndexMetadata.INDEX_UUID_NA_VALUE, null);
        context.setAllowUnmappedFields(false);
        MappedFieldType fieldType = new TextFieldMapper.TextFieldType("text");
        MappedFieldType result = context.failIfFieldMappingNotFound("name", fieldType);
        assertThat(result, sameInstance(fieldType));
        QueryShardException e = expectThrows(QueryShardException.class, () -> context.failIfFieldMappingNotFound("name", null));
        assertEquals("No field mapping can be found for the field with name [name]", e.getMessage());

        context.setAllowUnmappedFields(true);
        result = context.failIfFieldMappingNotFound("name", fieldType);
        assertThat(result, sameInstance(fieldType));
        result = context.failIfFieldMappingNotFound("name", null);
        assertThat(result, nullValue());

        context.setAllowUnmappedFields(false);
        context.setMapUnmappedFieldAsString(true);
        result = context.failIfFieldMappingNotFound("name", fieldType);
        assertThat(result, sameInstance(fieldType));
        result = context.failIfFieldMappingNotFound("name", null);
        assertThat(result, notNullValue());
        assertThat(result, instanceOf(TextFieldMapper.TextFieldType.class));
        assertThat(result.name(), equalTo("name"));
    }

    public void testToQueryFails() {
        QueryShardContext context = createQueryShardContext(IndexMetadata.INDEX_UUID_NA_VALUE, null);
        Exception exc = expectThrows(Exception.class,
            () -> context.toQuery(new AbstractQueryBuilder() {
                @Override
                public String getWriteableName() {
                    return null;
                }

                @Override
                protected void doWriteTo(StreamOutput out) throws IOException {

                }

                @Override
                protected void doXContent(XContentBuilder builder, Params params) throws IOException {

                }

                @Override
                protected Query doToQuery(QueryShardContext context) throws IOException {
                    throw new RuntimeException("boom");
                }

                @Override
                protected boolean doEquals(AbstractQueryBuilder other) {
                    return false;
                }

                @Override
                protected int doHashCode() {
                    return 0;
                }
            }));
        assertThat(exc.getMessage(), equalTo("failed to create query: boom"));
    }

    public void testClusterAlias() throws IOException {
        final String clusterAlias = randomBoolean() ? null : "remote_cluster";
        QueryShardContext context = createQueryShardContext(IndexMetadata.INDEX_UUID_NA_VALUE, clusterAlias);

        IndexFieldMapper mapper = new IndexFieldMapper();

        IndexFieldData<?> forField = context.getForField(mapper.fieldType());
        String expected = clusterAlias == null ? context.getIndexSettings().getIndexMetadata().getIndex().getName()
            : clusterAlias + ":" + context.getIndexSettings().getIndex().getName();
        assertEquals(expected, ((AbstractLeafOrdinalsFieldData)forField.load(null)).getOrdinalsValues().lookupOrd(0).utf8ToString());
    }

    public void testGetFullyQualifiedIndex() {
        String clusterAlias = randomAlphaOfLengthBetween(5, 10);
        String indexUuid = randomAlphaOfLengthBetween(3, 10);
        QueryShardContext shardContext = createQueryShardContext(indexUuid, clusterAlias);
        assertThat(shardContext.getFullyQualifiedIndex().getName(), equalTo(clusterAlias + ":index"));
        assertThat(shardContext.getFullyQualifiedIndex().getUUID(), equalTo(indexUuid));
    }

    public void testIndexSortedOnField() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put("index.sort.field", "sort_field")
            .build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("index")
            .settings(settings)
            .build();

        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        QueryShardContext context = new QueryShardContext(
            0, indexSettings, BigArrays.NON_RECYCLING_INSTANCE, null, null,
            null, null, null, NamedXContentRegistry.EMPTY, new NamedWriteableRegistry(Collections.emptyList()),
            null, null, () -> 0L, null, null, () -> true, null);

        assertTrue(context.indexSortedOnField("sort_field"));
        assertFalse(context.indexSortedOnField("second_sort_field"));
        assertFalse(context.indexSortedOnField("non_sort_field"));
    }

    public void testFielddataLookupSelfReference() {
        QueryShardContext queryShardContext = createQueryShardContext("uuid", null,
            mapperService -> when(mapperService.fieldType(any())).thenAnswer(inv -> {
                String name = (String) inv.getArguments()[0];
                MappedFieldType ft = mock(MappedFieldType.class);
                when(ft.name()).thenReturn(name);
                when(ft.fielddataBuilder(any(), any())).thenAnswer(inv2 -> {
                    @SuppressWarnings("unchecked")
                    Supplier<SearchLookup> searchLookupSupplier = (Supplier<SearchLookup>) inv2.getArguments()[1];
                    //simulate a runtime field that depends on itself e.g. field: doc['field']
                    return searchLookupSupplier.get().doc().getLeafDocLookup(null).get(ft.name());
                });
                return ft;
            }));
        String expectedMessage = "Cyclic dependency detected while resolving runtime fields: test -> test";
        MappedFieldType fieldType = queryShardContext.fieldMapper("test");
        {
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> queryShardContext.getForField(fieldType));
            assertEquals(expectedMessage, iae.getMessage());
        }
        {
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                () -> queryShardContext.lookup().doc().getLeafDocLookup(null).get("test"));
            assertEquals(expectedMessage, iae.getMessage());
        }
        {
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                () -> queryShardContext.lookup().doc().getForField(fieldType));
            assertEquals(expectedMessage, iae.getMessage());
        }
    }

    public void testFielddataLookupLooseLoop() {
        QueryShardContext queryShardContext = createQueryShardContext("uuid", null,
            mapperService -> when(mapperService.fieldType(any())).thenAnswer(inv -> {
                String name = (String) inv.getArguments()[0];
                MappedFieldType ft = mock(MappedFieldType.class);
                when(ft.name()).thenReturn(name);
                when(ft.fielddataBuilder(any(), any())).thenAnswer(inv2 -> {
                    @SuppressWarnings("unchecked")
                    Supplier<SearchLookup> searchLookupSupplier = (Supplier<SearchLookup>) inv2.getArguments()[1];
                    //simulate a runtime field cycle: a: doc['aa'] aa: doc['aaa'] aaa: doc['aaaa'] aaaa: doc['a']
                    if (ft.name().equals("aaaa")) {
                        return searchLookupSupplier.get().doc().getLeafDocLookup(null).get("a");
                    }
                    return searchLookupSupplier.get().doc().getLeafDocLookup(null).get(ft.name() + "a");
                });
                return ft;
            }));
        String expectedMessage = "Cyclic dependency detected while resolving runtime fields: a -> aa -> aaa -> aaaa -> a";
        MappedFieldType fieldType = queryShardContext.fieldMapper("a");
        {
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> queryShardContext.getForField(fieldType));
            assertEquals(expectedMessage, iae.getMessage());
        }
        {
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                () -> queryShardContext.lookup().doc().getLeafDocLookup(null).get("a"));
            assertEquals(expectedMessage, iae.getMessage());
        }
        {
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                () -> queryShardContext.lookup().doc().getForField(fieldType));
            assertEquals(expectedMessage, iae.getMessage());
        }
    }

    public void testFielddataLookupTerminatesInLoop() {
        QueryShardContext queryShardContext = createQueryShardContext("uuid", null,
            mapperService -> when(mapperService.fieldType(any())).thenAnswer(inv -> {
                String name = (String) inv.getArguments()[0];
                MappedFieldType ft = mock(MappedFieldType.class);
                when(ft.name()).thenReturn(name);
                when(ft.fielddataBuilder(any(), any())).thenAnswer(inv2 -> {
                    @SuppressWarnings("unchecked")
                    Supplier<SearchLookup> searchLookupSupplier = (Supplier<SearchLookup>) inv2.getArguments()[1];
                    //simulate a runtime field cycle: a: doc['aa'] aa: doc['aaa'] aaa: doc['aaaa'] aaaa: doc['aaaa']
                    if (ft.name().equals("aaaa")) {
                        return searchLookupSupplier.get().doc().getLeafDocLookup(null).get("aaaa");
                    }
                    return searchLookupSupplier.get().doc().getLeafDocLookup(null).get(ft.name() + "a");
                });
                return ft;
            }));
        String expectedMessage = "Cyclic dependency detected while resolving runtime fields: a -> aa -> aaa -> aaaa -> aaaa";;
        MappedFieldType fieldType = queryShardContext.fieldMapper("a");
        {
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> queryShardContext.getForField(fieldType));
            assertEquals(expectedMessage, iae.getMessage());
        }
        {
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                () -> queryShardContext.lookup().doc().getLeafDocLookup(null).get("a"));
            assertEquals(expectedMessage, iae.getMessage());
        }
        {
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                () -> queryShardContext.lookup().doc().getForField(fieldType));
            assertEquals(expectedMessage, iae.getMessage());
        }
    }

    public void testFielddataLookupTooManyReferences() {
        QueryShardContext queryShardContext = createQueryShardContext("uuid", null,
            mapperService -> when(mapperService.fieldType(any())).thenAnswer(inv -> {
                String name = (String) inv.getArguments()[0];
                MappedFieldType ft = mock(MappedFieldType.class);
                when(ft.name()).thenReturn(name);
                when(ft.fielddataBuilder(any(), any())).thenAnswer(inv2 -> {
                    @SuppressWarnings("unchecked")
                    Supplier<SearchLookup> searchLookupSupplier = (Supplier<SearchLookup>) inv2.getArguments()[1];
                    int i = Integer.parseInt(ft.name());
                    return searchLookupSupplier.get().doc().getLeafDocLookup(null).get(Integer.toString(i + 1));
                });
                return ft;
            }));
        String expectedMessage = "Field requires resolving too many dependent fields: 1 -> 2 -> 3 -> 4 -> 5 -> 6";
        MappedFieldType fieldType = queryShardContext.fieldMapper("1");
        {
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> queryShardContext.getForField(fieldType));
            assertEquals(expectedMessage, iae.getMessage());
        }
        {
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                () -> queryShardContext.lookup().doc().getLeafDocLookup(null).get("1"));
            assertEquals(expectedMessage, iae.getMessage());
        }
        {
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                () -> queryShardContext.lookup().doc().getForField(fieldType));
            assertEquals(expectedMessage, iae.getMessage());
        }
    }

    public static QueryShardContext createQueryShardContext(String indexUuid, String clusterAlias) {
        return createQueryShardContext(indexUuid, clusterAlias, mapperService -> {});
    }

    private static QueryShardContext createQueryShardContext(String indexUuid,
                                                             String clusterAlias,
                                                             Consumer<MapperService> mapperServiceConsumer) {
        IndexMetadata.Builder indexMetadataBuilder = new IndexMetadata.Builder("index");
        indexMetadataBuilder.settings(Settings.builder().put("index.version.created", Version.CURRENT)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
            .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
        );
        IndexMetadata indexMetadata = indexMetadataBuilder.build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        when(mapperService.index()).thenReturn(indexMetadata.getIndex());
        mapperServiceConsumer.accept(mapperService);
        final long nowInMillis = randomNonNegativeLong();
        return new QueryShardContext(
            0, indexSettings, BigArrays.NON_RECYCLING_INSTANCE, null,
                (mappedFieldType, idxName, searchLookup) -> mappedFieldType.fielddataBuilder(idxName, searchLookup).build(null, null, null),
                mapperService, null, null, NamedXContentRegistry.EMPTY, new NamedWriteableRegistry(Collections.emptyList()),
            null, null, () -> nowInMillis, clusterAlias, null, () -> true, null);
    }
}
