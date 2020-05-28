/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.ParametrizedFieldMapper.Parameter;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ParametrizedMapperTests extends ESSingleNodeTestCase {

    public static class TestPlugin extends Plugin implements MapperPlugin {
        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            return Map.of("test_mapper", new TypeParser());
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(TestPlugin.class);
    }

    private static class StringWrapper {
        final String name;

        private StringWrapper(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StringWrapper that = (StringWrapper) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }

    private static TestMapper toType(Mapper in) {
        return (TestMapper) in;
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {

        final Parameter<Boolean> fixed
            = Parameter.boolParam("fixed", false, m -> toType(m).fixed, true);
        final Parameter<Boolean> fixed2
            = Parameter.boolParam("fixed2", false, m -> toType(m).fixed2, false)
            .addDeprecatedName("fixed2_old");
        final Parameter<String> variable
            = Parameter.stringParam("variable", true, m -> toType(m).variable, "default").acceptsNull();
        final Parameter<StringWrapper> wrapper
            = new Parameter<>("wrapper", true, () -> new StringWrapper("default"),
            (n, c, o) -> {
                if (o == null) return null;
                return new StringWrapper(o.toString());
                },
            m -> toType(m).wrapper).setSerializer((b, n, v) -> b.field(n, v.name));
        final Parameter<Integer> intValue = Parameter.intParam("int_value", true, m -> toType(m).intValue, 5)
            .setValidator(n -> {
                if (n > 50) {
                    throw new IllegalArgumentException("Value of [n] cannot be greater than 50");
                }
            });
        final Parameter<NamedAnalyzer> analyzer
            = Parameter.analyzerParam("analyzer", true, m -> toType(m).analyzer, () -> Lucene.KEYWORD_ANALYZER);
        final Parameter<NamedAnalyzer> searchAnalyzer
            = Parameter.analyzerParam("search_analyzer", true, m -> toType(m).searchAnalyzer, analyzer::getValue);
        final Parameter<Boolean> index = Parameter.boolParam("index", false, m -> toType(m).index, true);
        final Parameter<String> required = Parameter.stringParam("required", true, m -> toType(m).required, null)
            .setValidator(value -> {
                if (value == null) {
                    throw new IllegalArgumentException("field [required] must be specified");
                }
            });

        protected Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(fixed, fixed2, variable, index, wrapper, intValue, analyzer, searchAnalyzer, required);
        }

        @Override
        public ParametrizedFieldMapper build(Mapper.BuilderContext context) {
            return new TestMapper(name(), buildFullName(context),
                multiFieldsBuilder.build(this, context), copyTo.build(), this);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            builder.parse(name, parserContext, node);
            return builder;
        }
    }

    public static class TestMapper extends ParametrizedFieldMapper {

        private final boolean fixed;
        private final boolean fixed2;
        private final String variable;
        private final StringWrapper wrapper;
        private final int intValue;
        private final NamedAnalyzer analyzer;
        private final NamedAnalyzer searchAnalyzer;
        private final boolean index;
        private final String required;

        protected TestMapper(String simpleName, String fullName, MultiFields multiFields, CopyTo copyTo,
                             ParametrizedMapperTests.Builder builder) {
            super(simpleName, new KeywordFieldMapper.KeywordFieldType(fullName), multiFields, copyTo);
            this.fixed = builder.fixed.getValue();
            this.fixed2 = builder.fixed2.getValue();
            this.variable = builder.variable.getValue();
            this.wrapper = builder.wrapper.getValue();
            this.intValue = builder.intValue.getValue();
            this.analyzer = builder.analyzer.getValue();
            this.searchAnalyzer = builder.searchAnalyzer.getValue();
            this.index = builder.index.getValue();
            this.required = builder.required.getValue();
        }

        @Override
        public Builder getMergeBuilder() {
            return new ParametrizedMapperTests.Builder(simpleName()).init(this);
        }

        @Override
        protected void parseCreateField(ParseContext context) {

        }

        @Override
        protected Object parseSourceValue(Object value) {
            return value;
        }

        @Override
        protected String contentType() {
            return "test_mapper";
        }
    }

    private static TestMapper fromMapping(String mapping, Version version) {
        MapperService mapperService = mock(MapperService.class);
        IndexAnalyzers indexAnalyzers = new IndexAnalyzers(
            Map.of("_standard", Lucene.STANDARD_ANALYZER,
                "_keyword", Lucene.KEYWORD_ANALYZER,
                "default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            Collections.emptyMap(), Collections.emptyMap());
        when(mapperService.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        Mapper.TypeParser.ParserContext pc = new Mapper.TypeParser.ParserContext(s -> null, mapperService, s -> {
            if (Objects.equals("keyword", s)) {
                return new KeywordFieldMapper.TypeParser();
            }
            if (Objects.equals("binary", s)) {
                return BinaryFieldMapper.PARSER;
            }
            return null;
        }, version, () -> null, null);
        return (TestMapper) new TypeParser()
            .parse("field", XContentHelper.convertToMap(JsonXContent.jsonXContent, mapping, true), pc)
            .build(new Mapper.BuilderContext(Settings.EMPTY, new ContentPath(0)));
    }

    private static TestMapper fromMapping(String mapping) {
        return fromMapping(mapping, Version.CURRENT);
    }

    // defaults - create empty builder config, and serialize with and without defaults
    public void testDefaults() throws IOException {
        String mapping = "{\"type\":\"test_mapper\",\"required\":\"value\"}";
        TestMapper mapper = fromMapping(mapping);

        assertTrue(mapper.fixed);
        assertEquals("default", mapper.variable);

        assertEquals("{\"field\":" + mapping + "}", Strings.toString(mapper));

        XContentBuilder builder = JsonXContent.contentBuilder();
        ToXContent.Params params = new ToXContent.MapParams(Map.of("include_defaults", "true"));
        builder.startObject();
        mapper.toXContent(builder, params);
        builder.endObject();
        assertEquals("{\"field\":{\"type\":\"test_mapper\",\"fixed\":true," +
                "\"fixed2\":false,\"variable\":\"default\",\"index\":true," +
                "\"wrapper\":\"default\",\"int_value\":5,\"analyzer\":\"_keyword\"," +
                "\"search_analyzer\":\"_keyword\",\"required\":\"value\"}}",
            Strings.toString(builder));
    }

    // merging - try updating 'fixed' and 'fixed2' should get an error, try updating 'variable' and verify update
    public void testMerging() {
        String mapping = "{\"type\":\"test_mapper\",\"fixed\":false,\"required\":\"value\"}";
        TestMapper mapper = fromMapping(mapping);
        assertEquals("{\"field\":" + mapping + "}", Strings.toString(mapper));

        TestMapper badMerge = fromMapping("{\"type\":\"test_mapper\",\"fixed\":true,\"fixed2\":true,\"required\":\"value\"}");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> mapper.merge(badMerge));
        String expectedError = "Mapper for [field] conflicts with existing mapper:\n" +
            "\tCannot update parameter [fixed] from [false] to [true]\n" +
            "\tCannot update parameter [fixed2] from [false] to [true]";
        assertEquals(expectedError, e.getMessage());

        assertEquals("{\"field\":" + mapping + "}", Strings.toString(mapper));   // original mapping is unaffected

        // TODO: should we have to include 'fixed' here? Or should updates take as 'defaults' the existing values?
        TestMapper goodMerge = fromMapping("{\"type\":\"test_mapper\",\"fixed\":false,\"variable\":\"updated\",\"required\":\"value\"}");
        TestMapper merged = (TestMapper) mapper.merge(goodMerge);

        assertEquals("{\"field\":" + mapping + "}", Strings.toString(mapper));   // original mapping is unaffected
        assertEquals("{\"field\":{\"type\":\"test_mapper\",\"fixed\":false,\"variable\":\"updated\",\"required\":\"value\"}}",
            Strings.toString(merged));
    }

    // add multifield, verify, add second multifield, verify, overwrite second multifield
    public void testMultifields() {
        String mapping = "{\"type\":\"test_mapper\",\"variable\":\"foo\",\"required\":\"value\"," +
            "\"fields\":{\"sub\":{\"type\":\"keyword\"}}}";
        TestMapper mapper = fromMapping(mapping);
        assertEquals("{\"field\":" + mapping + "}", Strings.toString(mapper));

        String addSubField = "{\"type\":\"test_mapper\",\"variable\":\"foo\",\"required\":\"value\"" +
            ",\"fields\":{\"sub2\":{\"type\":\"keyword\"}}}";
        TestMapper toMerge = fromMapping(addSubField);
        TestMapper merged = (TestMapper) mapper.merge(toMerge);
        assertEquals("{\"field\":{\"type\":\"test_mapper\",\"variable\":\"foo\",\"required\":\"value\"," +
            "\"fields\":{\"sub\":{\"type\":\"keyword\"},\"sub2\":{\"type\":\"keyword\"}}}}", Strings.toString(merged));

        String badSubField = "{\"type\":\"test_mapper\",\"variable\":\"foo\",\"required\":\"value\"," +
            "\"fields\":{\"sub2\":{\"type\":\"binary\"}}}";
        TestMapper badToMerge = fromMapping(badSubField);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merged.merge(badToMerge));
        assertEquals("mapper [field.sub2] cannot be changed from type [keyword] to [binary]", e.getMessage());
    }

    // add copy_to, verify
    public void testCopyTo() {
        String mapping = "{\"type\":\"test_mapper\",\"variable\":\"foo\",\"required\":\"value\",\"copy_to\":[\"other\"]}";
        TestMapper mapper = fromMapping(mapping);
        assertEquals("{\"field\":" + mapping + "}", Strings.toString(mapper));

        // On update, copy_to is completely replaced

        TestMapper toMerge = fromMapping("{\"type\":\"test_mapper\",\"variable\":\"updated\",\"required\":\"value\"," +
            "\"copy_to\":[\"foo\",\"bar\"]}");
        TestMapper merged = (TestMapper) mapper.merge(toMerge);
        assertEquals("{\"field\":{\"type\":\"test_mapper\",\"variable\":\"updated\",\"required\":\"value\"," +
                "\"copy_to\":[\"foo\",\"bar\"]}}",
            Strings.toString(merged));

        TestMapper removeCopyTo = fromMapping("{\"type\":\"test_mapper\",\"variable\":\"updated\",\"required\":\"value\"}");
        TestMapper noCopyTo = (TestMapper) merged.merge(removeCopyTo);
        assertEquals("{\"field\":{\"type\":\"test_mapper\",\"variable\":\"updated\",\"required\":\"value\"}}", Strings.toString(noCopyTo));
    }

    public void testNullables() {
        String mapping = "{\"type\":\"test_mapper\",\"fixed\":null,\"required\":\"value\"}";
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> fromMapping(mapping));
        assertEquals("[fixed] on mapper [field] of type [test_mapper] must not have a [null] value", e.getMessage());

        String fine = "{\"type\":\"test_mapper\",\"variable\":null,\"required\":\"value\"}";
        TestMapper mapper = fromMapping(fine);
        assertEquals("{\"field\":" + fine + "}", Strings.toString(mapper));
    }

    public void testObjectSerialization() throws IOException {

        IndexService indexService = createIndex("test");

        String mapping = "{\"_doc\":{" +
            "\"properties\":{" +
            "\"actual\":{\"type\":\"double\"}," +
            "\"bucket_count\":{\"type\":\"long\"}," +
            "\"bucket_influencers\":{\"type\":\"nested\",\"properties\":{" +
            "\"anomaly_score\":{\"type\":\"double\"}," +
            "\"bucket_span\":{\"type\":\"long\"}," +
            "\"is_interim\":{\"type\":\"boolean\"}}}}}}";
        indexService.mapperService().merge("_doc", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, Strings.toString(indexService.mapperService().documentMapper()));

        indexService.mapperService().merge("_doc", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, Strings.toString(indexService.mapperService().documentMapper()));
    }

    // test custom serializer
    public void testCustomSerialization() {
        String mapping = "{\"type\":\"test_mapper\",\"wrapper\":\"wrapped value\",\"required\":\"value\"}";
        TestMapper mapper = fromMapping(mapping);
        assertEquals("wrapped value", mapper.wrapper.name);
        assertEquals("{\"field\":" + mapping + "}", Strings.toString(mapper));
    }

    // test validator
    public void testParameterValidation() {
        String mapping = "{\"type\":\"test_mapper\",\"int_value\":10,\"required\":\"value\"}";
        TestMapper mapper = fromMapping(mapping);
        assertEquals(10, mapper.intValue);
        assertEquals("{\"field\":" + mapping + "}", Strings.toString(mapper));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> fromMapping("{\"type\":\"test_mapper\",\"int_value\":60,\"required\":\"value\"}"));
        assertEquals("Value of [n] cannot be greater than 50", e.getMessage());
    }

    // test deprecations
    public void testDeprecatedParameterName() {
        String mapping = "{\"type\":\"test_mapper\",\"fixed2_old\":true,\"required\":\"value\"}";
        TestMapper mapper = fromMapping(mapping);
        assertTrue(mapper.fixed2);
        assertWarnings("Parameter [fixed2_old] on mapper [field] is deprecated, use [fixed2]");
        assertEquals("{\"field\":{\"type\":\"test_mapper\",\"fixed2\":true,\"required\":\"value\"}}", Strings.toString(mapper));
    }

    public void testAnalyzers() {
        String mapping = "{\"type\":\"test_mapper\",\"analyzer\":\"_standard\",\"required\":\"value\"}";
        TestMapper mapper = fromMapping(mapping);
        assertEquals(mapper.analyzer, Lucene.STANDARD_ANALYZER);
        assertEquals("{\"field\":" + mapping + "}", Strings.toString(mapper));

        String withDef = "{\"type\":\"test_mapper\",\"analyzer\":\"default\",\"required\":\"value\"}";
        mapper = fromMapping(withDef);
        assertEquals(mapper.analyzer.name(), "default");
        assertThat(mapper.analyzer.analyzer(), instanceOf(StandardAnalyzer.class));
        assertEquals("{\"field\":" + withDef + "}", Strings.toString(mapper));

        String badAnalyzer = "{\"type\":\"test_mapper\",\"analyzer\":\"wibble\",\"required\":\"value\"}";
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> fromMapping(badAnalyzer));
        assertEquals("analyzer [wibble] has not been configured in mappings", e.getMessage());
    }

    public void testDeprecatedParameters() {
        // 'index' is declared explicitly, 'store' is not, but is one of the previously always-accepted params
        String mapping = "{\"type\":\"test_mapper\",\"index\":false,\"store\":true,\"required\":\"value\"}";
        TestMapper mapper = fromMapping(mapping, Version.V_7_8_0);
        assertWarnings("Parameter [store] has no effect on type [test_mapper] and will be removed in future");
        assertFalse(mapper.index);
        assertEquals("{\"field\":{\"type\":\"test_mapper\",\"index\":false,\"required\":\"value\"}}", Strings.toString(mapper));

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> fromMapping(mapping, Version.V_8_0_0));
        assertEquals("unknown parameter [store] on mapper [field] of type [test_mapper]", e.getMessage());
    }

    public void testLinkedAnalyzers() {
        String mapping = "{\"type\":\"test_mapper\",\"analyzer\":\"_standard\",\"required\":\"value\"}";
        TestMapper mapper = fromMapping(mapping);
        assertEquals("_standard", mapper.analyzer.name());
        assertEquals("_standard", mapper.searchAnalyzer.name());
        assertEquals("{\"field\":" + mapping + "}", Strings.toString(mapper));

        String mappingWithSA = "{\"type\":\"test_mapper\",\"search_analyzer\":\"_standard\",\"required\":\"value\"}";
        mapper = fromMapping(mappingWithSA);
        assertEquals("_keyword", mapper.analyzer.name());
        assertEquals("_standard", mapper.searchAnalyzer.name());
        assertEquals("{\"field\":" + mappingWithSA + "}", Strings.toString(mapper));

        String mappingWithBoth = "{\"type\":\"test_mapper\",\"analyzer\":\"default\"," +
            "\"search_analyzer\":\"_standard\",\"required\":\"value\"}";
        mapper = fromMapping(mappingWithBoth);
        assertEquals("default", mapper.analyzer.name());
        assertEquals("_standard", mapper.searchAnalyzer.name());
        assertEquals("{\"field\":" + mappingWithBoth + "}", Strings.toString(mapper));
    }

    public void testRequiredField() {
        {
            String mapping = "{\"type\":\"test_mapper\"}";
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> fromMapping(mapping));
            assertEquals("field [required] must be specified", iae.getMessage());
        }
        {
            String mapping = "{\"type\":\"test_mapper\",\"required\":null}";
            MapperParsingException exc = expectThrows(MapperParsingException.class, () -> fromMapping(mapping));
            assertEquals("[required] on mapper [field] of type [test_mapper] must not have a [null] value", exc.getMessage());
        }
    }
}
