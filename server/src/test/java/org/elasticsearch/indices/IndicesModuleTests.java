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

package org.elasticsearch.indices;

import org.elasticsearch.Version;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TypeFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

public class IndicesModuleTests extends ESTestCase {

    private static class FakeMapperParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {
            return null;
        }
    }

    private static class FakeMetadataMapperParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {
            return null;
        }
        @Override
        public MetadataFieldMapper getDefault(ParserContext context) {
            return null;
        }
    }

    private final List<MapperPlugin> fakePlugins = Arrays.asList(new MapperPlugin() {
        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            return Collections.singletonMap("fake-mapper", new FakeMapperParser());
        }
        @Override
        public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
            return Collections.singletonMap("fake-metadata-mapper", new FakeMetadataMapperParser());
        }
    });

    private static final String[] EXPECTED_METADATA_FIELDS = new String[]{ IgnoredFieldMapper.NAME, IdFieldMapper.NAME,
            RoutingFieldMapper.NAME, IndexFieldMapper.NAME, SourceFieldMapper.NAME, TypeFieldMapper.NAME,
            NestedPathFieldMapper.NAME, VersionFieldMapper.NAME, SeqNoFieldMapper.NAME, FieldNamesFieldMapper.NAME };

    public void testBuiltinMappers() {
        IndicesModule module = new IndicesModule(Collections.emptyList());
        {
            Version version = VersionUtils.randomVersionBetween(random(),
                Version.V_8_0_0, Version.CURRENT);
            assertFalse(module.getMapperRegistry().getMapperParsers().isEmpty());
            assertFalse(module.getMapperRegistry().getMetadataMapperParsers(version).isEmpty());
            Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers =
                module.getMapperRegistry().getMetadataMapperParsers(version);
            assertEquals(EXPECTED_METADATA_FIELDS.length, metadataMapperParsers.size());
            int i = 0;
            for (String field : metadataMapperParsers.keySet()) {
                assertEquals(EXPECTED_METADATA_FIELDS[i++], field);
            }
        }
        {
            Version version = VersionUtils.randomVersionBetween(random(),
                Version.V_7_0_0, VersionUtils.getPreviousVersion(Version.V_8_0_0));
            assertEquals(EXPECTED_METADATA_FIELDS.length - 1, module.getMapperRegistry().getMetadataMapperParsers(version).size());
        }
    }

    public void testBuiltinWithPlugins() {
        IndicesModule noPluginsModule = new IndicesModule(Collections.emptyList());
        IndicesModule module = new IndicesModule(fakePlugins);
        MapperRegistry registry = module.getMapperRegistry();
        assertThat(registry.getMapperParsers().size(), greaterThan(noPluginsModule.getMapperRegistry().getMapperParsers().size()));
        assertThat(registry.getMetadataMapperParsers(Version.CURRENT).size(),
                greaterThan(noPluginsModule.getMapperRegistry().getMetadataMapperParsers(Version.CURRENT).size()));
        Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers =
            module.getMapperRegistry().getMetadataMapperParsers(Version.CURRENT);
        Iterator<String> iterator = metadataMapperParsers.keySet().iterator();
        assertEquals(IgnoredFieldMapper.NAME, iterator.next());
        String last = null;
        while(iterator.hasNext()) {
            last = iterator.next();
        }
        assertEquals(FieldNamesFieldMapper.NAME, last);
    }

    public void testGetBuiltInMetadataFields() {
        Set<String> builtInMetadataFields = IndicesModule.getBuiltInMetadataFields();
        int i = 0;
        for (String field : builtInMetadataFields) {
            assertEquals(EXPECTED_METADATA_FIELDS[i++], field);
        }
    }

    public void testDuplicateBuiltinMapper() {
        List<MapperPlugin> plugins = Arrays.asList(new MapperPlugin() {
            @Override
            public Map<String, Mapper.TypeParser> getMappers() {
                return Collections.singletonMap(TextFieldMapper.CONTENT_TYPE, new FakeMapperParser());
            }
        });
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new IndicesModule(plugins));
        assertThat(e.getMessage(), containsString("already registered"));
    }

    public void testDuplicateOtherPluginMapper() {
        MapperPlugin plugin = new MapperPlugin() {
            @Override
            public Map<String, Mapper.TypeParser> getMappers() {
                return Collections.singletonMap("foo", new FakeMapperParser());
            }
        };
        List<MapperPlugin> plugins = Arrays.asList(plugin, plugin);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new IndicesModule(plugins));
        assertThat(e.getMessage(), containsString("already registered"));
    }

    public void testDuplicateBuiltinMetadataMapper() {
        List<MapperPlugin> plugins = Arrays.asList(new MapperPlugin() {
            @Override
            public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
                return Collections.singletonMap(IdFieldMapper.NAME, new FakeMetadataMapperParser());
            }
        });
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new IndicesModule(plugins));
        assertThat(e.getMessage(), containsString("already registered"));
    }

    public void testDuplicateOtherPluginMetadataMapper() {
        MapperPlugin plugin = new MapperPlugin() {
            @Override
            public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
                return Collections.singletonMap("foo", new FakeMetadataMapperParser());
            }
        };
        List<MapperPlugin> plugins = Arrays.asList(plugin, plugin);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new IndicesModule(plugins));
        assertThat(e.getMessage(), containsString("already registered"));
    }

    public void testDuplicateFieldNamesMapper() {
        List<MapperPlugin> plugins = Arrays.asList(new MapperPlugin() {
            @Override
            public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
                return Collections.singletonMap(FieldNamesFieldMapper.NAME, new FakeMetadataMapperParser());
            }
        });
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new IndicesModule(plugins));
        assertThat(e.getMessage(), containsString("cannot contain metadata mapper [_field_names]"));
    }

    public void testFieldNamesIsLast() {
        IndicesModule module = new IndicesModule(Collections.emptyList());
        Version version = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
        List<String> fieldNames = new ArrayList<>(module.getMapperRegistry().getMetadataMapperParsers(version).keySet());
        assertEquals(FieldNamesFieldMapper.NAME, fieldNames.get(fieldNames.size() - 1));
    }

    public void testFieldNamesIsLastWithPlugins() {
        IndicesModule module = new IndicesModule(fakePlugins);
        Version version = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
        List<String> fieldNames = new ArrayList<>(module.getMapperRegistry().getMetadataMapperParsers(version).keySet());
        assertEquals(FieldNamesFieldMapper.NAME, fieldNames.get(fieldNames.size() - 1));
    }

    public void testGetFieldFilter() {
        List<MapperPlugin> mapperPlugins = Arrays.asList(
            new MapperPlugin() {
                @Override
                public Function<String, Predicate<String>> getFieldFilter() {
                    return MapperPlugin.NOOP_FIELD_FILTER;
                }
            },
            new MapperPlugin() {
                @Override
                public Function<String, Predicate<String>> getFieldFilter() {
                    return index -> index.equals("hidden_index") ? field -> false : MapperPlugin.NOOP_FIELD_PREDICATE;
                }
            },
            new MapperPlugin() {
                @Override
                public Function<String, Predicate<String>> getFieldFilter() {
                    return index -> field -> field.equals("hidden_field") == false;
                }
            },
            new MapperPlugin() {
                @Override
                public Function<String, Predicate<String>> getFieldFilter() {
                    return index -> index.equals("filtered") ? field ->  field.equals("visible") : MapperPlugin.NOOP_FIELD_PREDICATE;
                }
            });

        IndicesModule indicesModule = new IndicesModule(mapperPlugins);
        MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();
        Function<String, Predicate<String>> fieldFilter = mapperRegistry.getFieldFilter();
        assertNotSame(MapperPlugin.NOOP_FIELD_FILTER, fieldFilter);

        assertFalse(fieldFilter.apply("hidden_index").test(randomAlphaOfLengthBetween(3, 5)));
        assertTrue(fieldFilter.apply(randomAlphaOfLengthBetween(3, 5)).test(randomAlphaOfLengthBetween(3, 5)));

        assertFalse(fieldFilter.apply(randomAlphaOfLengthBetween(3, 5)).test("hidden_field"));
        assertFalse(fieldFilter.apply("filtered").test(randomAlphaOfLengthBetween(3, 5)));
        assertFalse(fieldFilter.apply("filtered").test("hidden_field"));
        assertTrue(fieldFilter.apply("filtered").test("visible"));
        assertFalse(fieldFilter.apply("hidden_index").test("visible"));
        assertTrue(fieldFilter.apply(randomAlphaOfLengthBetween(3, 5)).test("visible"));
        assertFalse(fieldFilter.apply("hidden_index").test("hidden_field"));
    }

    public void testDefaultFieldFilterIsNoOp() {
        int numPlugins = randomIntBetween(0, 10);
        List<MapperPlugin> mapperPlugins = new ArrayList<>(numPlugins);
        for (int i = 0; i < numPlugins; i++) {
            mapperPlugins.add(new MapperPlugin() {});
        }
        IndicesModule indicesModule = new IndicesModule(mapperPlugins);
        Function<String, Predicate<String>> fieldFilter = indicesModule.getMapperRegistry().getFieldFilter();
        assertSame(MapperPlugin.NOOP_FIELD_FILTER, fieldFilter);
    }

    public void testNoOpFieldPredicate() {
        List<MapperPlugin> mapperPlugins = Arrays.asList(
                new MapperPlugin() {
                    @Override
                    public Function<String, Predicate<String>> getFieldFilter() {
                        return MapperPlugin.NOOP_FIELD_FILTER;
                    }
                },
                new MapperPlugin() {
                    @Override
                    public Function<String, Predicate<String>> getFieldFilter() {
                        return index -> index.equals("hidden_index") ? field -> false : MapperPlugin.NOOP_FIELD_PREDICATE;
                    }
                },
                new MapperPlugin() {
                    @Override
                    public Function<String, Predicate<String>> getFieldFilter() {
                        return index -> index.equals("filtered") ? field ->  field.equals("visible") : MapperPlugin.NOOP_FIELD_PREDICATE;
                    }
                });

        IndicesModule indicesModule = new IndicesModule(mapperPlugins);
        MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();
        Function<String, Predicate<String>> fieldFilter = mapperRegistry.getFieldFilter();
        assertSame(MapperPlugin.NOOP_FIELD_PREDICATE, fieldFilter.apply(randomAlphaOfLengthBetween(3, 7)));
        assertNotSame(MapperPlugin.NOOP_FIELD_PREDICATE, fieldFilter.apply("hidden_index"));
        assertNotSame(MapperPlugin.NOOP_FIELD_PREDICATE, fieldFilter.apply("filtered"));
    }
}
