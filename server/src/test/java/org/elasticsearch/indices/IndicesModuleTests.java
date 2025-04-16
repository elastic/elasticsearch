/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DocCountFieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.IndexModeFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.RuntimeField;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.test.LambdaMatchers.falseWith;
import static org.elasticsearch.test.LambdaMatchers.trueWith;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class IndicesModuleTests extends ESTestCase {

    private static class FakeMapperParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext)
            throws MapperParsingException {
            return null;
        }
    }

    private static final MetadataFieldMapper.TypeParser PARSER = new MetadataFieldMapper.ConfigurableTypeParser(c -> null, c -> null);

    private final List<MapperPlugin> fakePlugins = Arrays.asList(new MapperPlugin() {
        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            return Collections.singletonMap("fake-mapper", new FakeMapperParser());
        }

        @Override
        public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
            return Collections.singletonMap("fake-metadata-mapper", PARSER);
        }
    });

    private static final String[] EXPECTED_METADATA_FIELDS = new String[] {
        IgnoredFieldMapper.NAME,
        IdFieldMapper.NAME,
        RoutingFieldMapper.NAME,
        TimeSeriesIdFieldMapper.NAME,
        TimeSeriesRoutingHashFieldMapper.NAME,
        IndexFieldMapper.NAME,
        IndexModeFieldMapper.NAME,
        SourceFieldMapper.NAME,
        IgnoredSourceFieldMapper.NAME,
        NestedPathFieldMapper.NAME,
        VersionFieldMapper.NAME,
        SeqNoFieldMapper.NAME,
        DocCountFieldMapper.NAME,
        DataStreamTimestampFieldMapper.NAME,
        FieldNamesFieldMapper.NAME };

    public void testBuiltinMappers() {
        IndicesModule module = new IndicesModule(Collections.emptyList());
        {
            IndexVersion version = IndexVersionUtils.randomVersionBetween(random(), IndexVersions.V_8_0_0, IndexVersion.current());
            assertThat(
                module.getMapperRegistry().getMapperParser("object", IndexVersion.current()),
                instanceOf(ObjectMapper.TypeParser.class)
            );
            assertFalse(module.getMapperRegistry().getMetadataMapperParsers(version).isEmpty());
            Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers = module.getMapperRegistry()
                .getMetadataMapperParsers(version);
            assertEquals(EXPECTED_METADATA_FIELDS.length, metadataMapperParsers.size());
            int i = 0;
            for (String field : metadataMapperParsers.keySet()) {
                assertEquals(EXPECTED_METADATA_FIELDS[i++], field);
            }
        }
        {
            IndexVersion version = IndexVersionUtils.randomVersionBetween(
                random(),
                IndexVersions.V_7_0_0,
                IndexVersionUtils.getPreviousVersion(IndexVersions.V_8_0_0)
            );
            assertEquals(EXPECTED_METADATA_FIELDS.length - 1, module.getMapperRegistry().getMetadataMapperParsers(version).size());
        }
    }

    public void testBuiltinWithPlugins() {
        IndicesModule noPluginsModule = new IndicesModule(Collections.emptyList());
        IndicesModule module = new IndicesModule(fakePlugins);
        MapperRegistry registry = module.getMapperRegistry();
        assertThat(registry.getMapperParser("fake-mapper", IndexVersion.current()), instanceOf(FakeMapperParser.class));
        assertNull(noPluginsModule.getMapperRegistry().getMapperParser("fake-mapper", IndexVersion.current()));
        assertThat(
            registry.getMetadataMapperParsers(IndexVersion.current()).size(),
            greaterThan(noPluginsModule.getMapperRegistry().getMetadataMapperParsers(IndexVersion.current()).size())
        );
        Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers = module.getMapperRegistry()
            .getMetadataMapperParsers(IndexVersion.current());
        Iterator<String> iterator = metadataMapperParsers.keySet().iterator();
        assertEquals(IgnoredFieldMapper.NAME, iterator.next());
        String last = null;
        while (iterator.hasNext()) {
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
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new IndicesModule(plugins));
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
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new IndicesModule(plugins));
        assertThat(e.getMessage(), containsString("already registered"));
    }

    public void testDuplicateBuiltinMetadataMapper() {
        List<MapperPlugin> plugins = Arrays.asList(new MapperPlugin() {
            @Override
            public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
                return Collections.singletonMap(IdFieldMapper.NAME, PARSER);
            }
        });
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new IndicesModule(plugins));
        assertThat(e.getMessage(), containsString("already registered"));
    }

    public void testDuplicateOtherPluginMetadataMapper() {
        MapperPlugin plugin = new MapperPlugin() {
            @Override
            public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
                return Collections.singletonMap("foo", PARSER);
            }
        };
        List<MapperPlugin> plugins = Arrays.asList(plugin, plugin);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new IndicesModule(plugins));
        assertThat(e.getMessage(), containsString("already registered"));
    }

    public void testDuplicateRuntimeFieldPlugin() {
        MapperPlugin plugin = new MapperPlugin() {
            @Override
            public Map<String, RuntimeField.Parser> getRuntimeFields() {
                return Map.of("test", new RuntimeField.Parser(name -> null));
            }
        };
        List<MapperPlugin> plugins = Arrays.asList(plugin, plugin);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new IndicesModule(plugins));
        assertThat(e.getMessage(), containsString("already registered"));
    }

    public void testRuntimeFieldPluginWithBuiltinFieldType() {
        MapperPlugin plugin = new MapperPlugin() {
            @Override
            public Map<String, RuntimeField.Parser> getRuntimeFields() {
                return Map.of(KeywordFieldMapper.CONTENT_TYPE, new RuntimeField.Parser(name -> null));
            }
        };
        List<MapperPlugin> plugins = Collections.singletonList(plugin);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new IndicesModule(plugins));
        assertThat(e.getMessage(), containsString("already registered"));
    }

    public void testDuplicateFieldNamesMapper() {
        List<MapperPlugin> plugins = Arrays.asList(new MapperPlugin() {
            @Override
            public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
                return Collections.singletonMap(FieldNamesFieldMapper.NAME, PARSER);
            }
        });
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new IndicesModule(plugins));
        assertThat(e.getMessage(), containsString("cannot contain metadata mapper [_field_names]"));
    }

    public void testFieldNamesIsLast() {
        IndicesModule module = new IndicesModule(Collections.emptyList());
        IndexVersion version = IndexVersionUtils.randomVersion();
        List<String> fieldNames = new ArrayList<>(module.getMapperRegistry().getMetadataMapperParsers(version).keySet());
        assertEquals(FieldNamesFieldMapper.NAME, fieldNames.get(fieldNames.size() - 1));
    }

    public void testFieldNamesIsLastWithPlugins() {
        IndicesModule module = new IndicesModule(fakePlugins);
        IndexVersion version = IndexVersionUtils.randomVersion();
        List<String> fieldNames = new ArrayList<>(module.getMapperRegistry().getMetadataMapperParsers(version).keySet());
        assertEquals(FieldNamesFieldMapper.NAME, fieldNames.get(fieldNames.size() - 1));
    }

    public void testGetFieldFilter() {
        List<MapperPlugin> mapperPlugins = List.of(new MapperPlugin() {
        }, new MapperPlugin() {
            @Override
            public Function<String, FieldPredicate> getFieldFilter() {
                return index -> index.equals("hidden_index") ? HIDDEN_INDEX : FieldPredicate.ACCEPT_ALL;
            }
        }, new MapperPlugin() {
            @Override
            public Function<String, FieldPredicate> getFieldFilter() {
                return index -> HIDDEN_FIELD;
            }
        }, new MapperPlugin() {
            @Override
            public Function<String, FieldPredicate> getFieldFilter() {
                return index -> index.equals("filtered") ? ONLY_VISIBLE : FieldPredicate.ACCEPT_ALL;
            }
        });

        IndicesModule indicesModule = new IndicesModule(mapperPlugins);
        MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();
        Function<String, FieldPredicate> fieldFilter = mapperRegistry.getFieldFilter();
        assertNotSame(MapperPlugin.NOOP_FIELD_FILTER, fieldFilter);

        assertThat(fieldFilter.apply("hidden_index"), falseWith(randomAlphaOfLengthBetween(3, 5)));
        assertThat(fieldFilter.apply(randomAlphaOfLengthBetween(3, 5)), trueWith(randomAlphaOfLengthBetween(3, 5)));

        assertThat(fieldFilter.apply(randomAlphaOfLengthBetween(3, 5)), falseWith("hidden_field"));
        assertThat(fieldFilter.apply("filtered"), falseWith(randomAlphaOfLengthBetween(3, 5)));
        assertThat(fieldFilter.apply("filtered"), falseWith("hidden_field"));
        assertThat(fieldFilter.apply("filtered"), trueWith("visible"));
        assertThat(fieldFilter.apply("hidden_index"), falseWith("visible"));
        assertThat(fieldFilter.apply(randomAlphaOfLengthBetween(3, 5)), trueWith("visible"));
        assertThat(fieldFilter.apply("hidden_index"), falseWith("hidden_field"));

        assertThat(fieldFilter.apply("filtered").modifyHash("hash"), equalTo("only-visible:hide-field:hash"));
        assertThat(fieldFilter.apply(randomAlphaOfLengthBetween(3, 5)).modifyHash("hash"), equalTo("hide-field:hash"));
        assertThat(fieldFilter.apply("hidden_index").modifyHash("hash"), equalTo("hide-field:hidden:hash"));
    }

    public void testDefaultFieldFilterIsNoOp() {
        int numPlugins = randomIntBetween(0, 10);
        List<MapperPlugin> mapperPlugins = new ArrayList<>(numPlugins);
        for (int i = 0; i < numPlugins; i++) {
            mapperPlugins.add(new MapperPlugin() {
            });
        }
        IndicesModule indicesModule = new IndicesModule(mapperPlugins);
        Function<String, FieldPredicate> fieldFilter = indicesModule.getMapperRegistry().getFieldFilter();
        assertSame(MapperPlugin.NOOP_FIELD_FILTER, fieldFilter);
    }

    public void testNoOpFieldPredicate() {
        List<MapperPlugin> mapperPlugins = Arrays.asList(new MapperPlugin() {
        }, new MapperPlugin() {
            @Override
            public Function<String, FieldPredicate> getFieldFilter() {
                return index -> index.equals("hidden_index") ? HIDDEN_INDEX : FieldPredicate.ACCEPT_ALL;
            }
        }, new MapperPlugin() {
            @Override
            public Function<String, FieldPredicate> getFieldFilter() {
                return index -> index.equals("filtered") ? ONLY_VISIBLE : FieldPredicate.ACCEPT_ALL;
            }
        });

        IndicesModule indicesModule = new IndicesModule(mapperPlugins);
        MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();
        Function<String, FieldPredicate> fieldFilter = mapperRegistry.getFieldFilter();
        assertSame(FieldPredicate.ACCEPT_ALL, fieldFilter.apply(randomAlphaOfLengthBetween(3, 7)));
        assertNotSame(FieldPredicate.ACCEPT_ALL, fieldFilter.apply("hidden_index"));
        assertNotSame(FieldPredicate.ACCEPT_ALL, fieldFilter.apply("filtered"));
    }

    private static final FieldPredicate HIDDEN_INDEX = new FieldPredicate() {
        @Override
        public boolean test(String field) {
            return false;
        }

        @Override
        public String modifyHash(String hash) {
            return "hidden:" + hash;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }
    };

    private static final FieldPredicate HIDDEN_FIELD = new FieldPredicate() {
        @Override
        public boolean test(String field) {
            return false == field.equals("hidden_field");
        }

        @Override
        public String modifyHash(String hash) {
            return "hide-field:" + hash;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }
    };

    private static final FieldPredicate ONLY_VISIBLE = new FieldPredicate() {
        @Override
        public boolean test(String field) {
            return field.equals("visible");
        }

        @Override
        public String modifyHash(String hash) {
            return "only-visible:" + hash;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }
    };
}
