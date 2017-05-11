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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

public class IndicesModuleTests extends ESTestCase {

    private static class FakeMapperParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {
            return null;
        }
    }

    private static class FakeMetadataMapperParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {
            return null;
        }
        @Override
        public MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext context) {
            return null;
        }
    }

    List<MapperPlugin> fakePlugins = Arrays.asList(new MapperPlugin() {
        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            return Collections.singletonMap("fake-mapper", new FakeMapperParser());
        }
        @Override
        public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
            return Collections.singletonMap("fake-metadata-mapper", new FakeMetadataMapperParser());
        }
    });

    public void testBuiltinMappers() {
        IndicesModule module = new IndicesModule(Collections.emptyList());
        assertFalse(module.getMapperRegistry().getMapperParsers().isEmpty());
        assertFalse(module.getMapperRegistry().getMetadataMapperParsers().isEmpty());
    }

    public void testBuiltinWithPlugins() {
        IndicesModule module = new IndicesModule(fakePlugins);
        MapperRegistry registry = module.getMapperRegistry();
        assertThat(registry.getMapperParsers().size(), Matchers.greaterThan(1));
        assertThat(registry.getMetadataMapperParsers().size(), Matchers.greaterThan(1));
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
        assertThat(e.getMessage(), Matchers.containsString("already registered"));
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
        assertThat(e.getMessage(), Matchers.containsString("already registered"));
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
        assertThat(e.getMessage(), Matchers.containsString("already registered"));
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
        assertThat(e.getMessage(), Matchers.containsString("already registered"));
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
        assertThat(e.getMessage(), Matchers.containsString("cannot contain metadata mapper [_field_names]"));
    }

    public void testFieldNamesIsLast() {
        IndicesModule module = new IndicesModule(Collections.emptyList());
        List<String> fieldNames = module.getMapperRegistry().getMetadataMapperParsers().keySet()
            .stream().collect(Collectors.toList());
        assertEquals(FieldNamesFieldMapper.NAME, fieldNames.get(fieldNames.size() - 1));
    }

    public void testFieldNamesIsLastWithPlugins() {
        IndicesModule module = new IndicesModule(fakePlugins);
        List<String> fieldNames = module.getMapperRegistry().getMetadataMapperParsers().keySet()
            .stream().collect(Collectors.toList());
        assertEquals(FieldNamesFieldMapper.NAME, fieldNames.get(fieldNames.size() - 1));
    }
}
