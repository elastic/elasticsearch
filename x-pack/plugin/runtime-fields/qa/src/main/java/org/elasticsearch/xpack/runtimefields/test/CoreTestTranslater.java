/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.runtimefields.test;

import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.section.ApiCallSection;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSuite;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.elasticsearch.test.rest.yaml.section.SetupSection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Builds test parameters similarly to {@link ESClientYamlSuiteTestCase#createParameters()},
 * replacing all fields with runtime fields that load from {@code _source} if possible. Tests
 * that configure the field in a way that are not supported by runtime fields are skipped.
 */
public abstract class CoreTestTranslater {
    public Iterable<Object[]> parameters() throws Exception {
        Map<String, Suite> suites = new HashMap<>();
        List<Object[]> result = new ArrayList<>();
        for (Object[] orig : ESClientYamlSuiteTestCase.createParameters()) {
            assert orig.length == 1;
            ClientYamlTestCandidate candidate = (ClientYamlTestCandidate) orig[0];
            Suite suite = suites.computeIfAbsent(candidate.getSuitePath(), k -> suite(candidate));
            if (suite.modified == null) {
                // The setup section contains an unsupported option
                continue;
            }
            if (false == suite.modifySections(candidate.getTestSection().getExecutableSections())) {
                // The test section contains an unsupported option
                continue;
            }
            ClientYamlTestSection modified = new ClientYamlTestSection(
                candidate.getTestSection().getLocation(),
                candidate.getTestSection().getName(),
                candidate.getTestSection().getSkipSection(),
                candidate.getTestSection().getExecutableSections()
            );
            result.add(new Object[] { new ClientYamlTestCandidate(suite.modified, modified) });
        }
        return result;
    }

    protected abstract Suite suite(ClientYamlTestCandidate candidate);

    private static String painlessToLoadFromSource(String name, String type) {
        String emit = PAINLESS_TO_EMIT.get(type);
        if (emit == null) {
            return null;
        }
        StringBuilder b = new StringBuilder();
        b.append("def v = params._source['").append(name).append("'];\n");
        b.append("if (v instanceof Iterable) {\n");
        b.append("  for (def vv : ((Iterable) v)) {\n");
        b.append("    if (vv != null) {\n");
        b.append("      def value = vv;\n");
        b.append("      ").append(emit).append("\n");
        b.append("    }\n");
        b.append("  }\n");
        b.append("} else {\n");
        b.append("  if (v != null) {\n");
        b.append("    def value = v;\n");
        b.append("    ").append(emit).append("\n");
        b.append("  }\n");
        b.append("}\n");
        return b.toString();
    }

    private static final Map<String, String> PAINLESS_TO_EMIT = Map.ofEntries(
        Map.entry(BooleanFieldMapper.CONTENT_TYPE, "emit(Boolean.parseBoolean(value.toString()));"),
        Map.entry(DateFieldMapper.CONTENT_TYPE, "emit(parse(value.toString()));"),
        Map.entry(
            NumberType.DOUBLE.typeName(),
            "emit(value instanceof Number ? ((Number) value).doubleValue() : Double.parseDouble(value.toString()));"
        ),
        Map.entry(KeywordFieldMapper.CONTENT_TYPE, "emit(value.toString());"),
        Map.entry(IpFieldMapper.CONTENT_TYPE, "emit(value.toString());"),
        Map.entry(
            NumberType.LONG.typeName(),
            "emit(value instanceof Number ? ((Number) value).longValue() : Long.parseLong(value.toString()));"
        )
    );

    protected abstract Map<String, Object> indexTemplate();

    protected static Map<String, Object> indexTemplateToDisableAllFields() {
        return Map.of("settings", Map.of(), "mappings", Map.of("dynamic", false));
    }

    protected static Map<String, Object> indexTemplateToAddRuntimeFieldsToMappings() {
        List<Map<String, Object>> dynamicTemplates = new ArrayList<>();
        for (String type : PAINLESS_TO_EMIT.keySet()) {
            if (type.equals("ip")) {
                // There isn't a dynamic template to pick up ips. They'll just look like strings.
                continue;
            }
            Map<String, Object> mapping = Map.ofEntries(
                Map.entry("type", "runtime"),
                Map.entry("runtime_type", type),
                Map.entry("script", painlessToLoadFromSource("{name}", type))
            );
            if (type.contentEquals("keyword")) {
                /*
                 * For "string"-type dynamic mappings emulate our default
                 * behavior with a top level text field and a `.keyword`
                 * multi-field. But instead of the default, use a runtime
                 * field for the multi-field.
                 */
                mapping = Map.of("type", "text", "fields", Map.of("keyword", mapping));
                dynamicTemplates.add(Map.of(type, Map.of("match_mapping_type", "string", "mapping", mapping)));
            } else {
                dynamicTemplates.add(Map.of(type, Map.of("match_mapping_type", type, "mapping", mapping)));
            }
        }
        return Map.of("settings", Map.of(), "mappings", Map.of("dynamic_templates", dynamicTemplates));
    }

    private ExecutableSection addIndexTemplate() {
        return new ExecutableSection() {
            @Override
            public XContentLocation getLocation() {
                return new XContentLocation(-1, -1);
            }

            @Override
            public void execute(ClientYamlTestExecutionContext executionContext) throws IOException {
                Map<String, String> params = Map.of("name", "hack_dynamic_mappings", "create", "true");
                List<Map<String, Object>> bodies = List.of(
                    Map.ofEntries(
                        Map.entry("index_patterns", "*"),
                        Map.entry("priority", Integer.MAX_VALUE - 1),
                        Map.entry("template", indexTemplate())
                    )
                );
                ClientYamlTestResponse response = executionContext.callApi("indices.put_index_template", params, bodies, Map.of());
                assertThat(response.getStatusCode(), equalTo(200));
                // There are probably some warning about overlapping templates. Ignore them.
            }
        };
    }

    /**
     * A modified suite.
     */
    protected abstract class Suite {
        private final ClientYamlTestSuite modified;

        public Suite(ClientYamlTestCandidate candidate) {
            if (false == modifySections(candidate.getSetupSection().getExecutableSections())) {
                modified = null;
                return;
            }
            /*
             * Modify the setup section to rewrite and create index commands and
             * to add a dynamic template that sets up any dynamic indices how we
             * expect them.
             */
            List<ExecutableSection> setup = new ArrayList<>(candidate.getSetupSection().getExecutableSections().size() + 1);
            setup.add(addIndexTemplate());
            setup.addAll(candidate.getSetupSection().getExecutableSections());
            modified = new ClientYamlTestSuite(
                candidate.getApi(),
                candidate.getName(),
                new SetupSection(candidate.getSetupSection().getSkipSection(), setup),
                candidate.getTeardownSection(),
                List.of()
            );
        }

        /**
         * Replace field configuration in {@code indices.create} with scripts
         * that load from the source.
         *
         * @return true if the section is appropriate for testing with runtime fields
         */
        public boolean modifySections(List<ExecutableSection> executables) {
            for (ExecutableSection section : executables) {
                if (false == (section instanceof DoSection)) {
                    continue;
                }
                DoSection doSection = (DoSection) section;
                String api = doSection.getApiCallSection().getApi();
                if (api.equals("indices.create")) {
                    if (false == modifyCreateIndex(doSection.getApiCallSection())) {
                        return false;
                    }
                }
                if (api.equals("search") || api.equals("async_search.submit")) {
                    if (false == modifySearch(doSection.getApiCallSection())) {
                        return false;
                    }
                }
            }
            return true;
        }

        /**
         * Modify a test search request.
         */
        protected abstract boolean modifySearch(ApiCallSection search);

        private boolean modifyCreateIndex(ApiCallSection createIndex) {
            String index = createIndex.getParams().get("index");
            for (Map<?, ?> body : createIndex.getBodies()) {
                Object settings = body.get("settings");
                if (settings instanceof Map && ((Map<?, ?>) settings).containsKey("sort.field")) {
                    /*
                     * You can't sort the index on a runtime_keyword and it is
                     * hard to figure out if the sort was a runtime_keyword so
                     * let's just skip this test.
                     */
                    continue;
                }
                Object mapping = body.get("mappings");
                if (false == (mapping instanceof Map)) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> mappingMap = (Map<String, Object>) mapping;
                if (false == modifyMapping(index, mappingMap)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Modify the mapping defined in the test.
         */
        protected abstract boolean modifyMapping(String index, Map<String, Object> mapping);

        /**
         * Modify the provided map in place, translating all fields into
         * runtime fields that load from source.
         * @return true if this mapping supports runtime fields, false otherwise
         */
        protected final boolean runtimeifyMappingProperties(
            Map<String, Object> properties,
            Map<String, Object> untouchedProperties,
            Map<String, Map<String, Object>> runtimeProperties
        ) {
            for (Map.Entry<String, Object> property : properties.entrySet()) {
                if (false == property.getValue() instanceof Map) {
                    untouchedProperties.put(property.getKey(), property.getValue());
                    continue;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> propertyMap = (Map<String, Object>) property.getValue();
                String name = property.getKey().toString();
                String type = Objects.toString(propertyMap.get("type"));
                if ("nested".equals(type)) {
                    // Our loading scripts can't be made to manage nested fields so we have to skip those tests.
                    return false;
                }
                if ("false".equals(Objects.toString(propertyMap.get("doc_values")))) {
                    // If doc_values is false we can't emulate with scripts. So we keep the old definition. `null` and `true` are fine.
                    untouchedProperties.put(property.getKey(), property.getValue());
                    continue;
                }
                if ("false".equals(Objects.toString(propertyMap.get("index")))) {
                    // If index is false we can't emulate with scripts
                    untouchedProperties.put(property.getKey(), property.getValue());
                    continue;
                }
                if ("true".equals(Objects.toString(propertyMap.get("store")))) {
                    // If store is true we can't emulate with scripts
                    untouchedProperties.put(property.getKey(), property.getValue());
                    continue;
                }
                if (propertyMap.containsKey("ignore_above")) {
                    // Scripts don't support ignore_above so we skip those fields
                    untouchedProperties.put(property.getKey(), property.getValue());
                    continue;
                }
                if (propertyMap.containsKey("ignore_malformed")) {
                    // Our source reading script doesn't emulate ignore_malformed
                    untouchedProperties.put(property.getKey(), property.getValue());
                    continue;
                }
                String toLoad = painlessToLoadFromSource(name, type);
                if (toLoad == null) {
                    untouchedProperties.put(property.getKey(), property.getValue());
                    continue;
                }
                Map<String, Object> runtimeConfig = new HashMap<>(propertyMap);
                runtimeConfig.put("script", toLoad);
                runtimeConfig.remove("store");
                runtimeConfig.remove("index");
                runtimeConfig.remove("doc_values");
                runtimeProperties.put(property.getKey(), runtimeConfig);
            }
            /*
             * Its tempting to return false here if we didn't make any runtime
             * fields, skipping the test. But that would cause us to skip any
             * test uses dynamic mappings. Disaster! Instead we use a dynamic
             * template to make the dynamic mappings into runtime fields too.
             * The downside is that we can run tests that don't use runtime
             * fields at all. That's unfortunate, but its ok.
             */
            return true;
        }
    }
}
