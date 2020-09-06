/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
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

public class CoreTestsWithRuntimeFieldsIT extends ESClientYamlSuiteTestCase {
    public CoreTestsWithRuntimeFieldsIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    /**
     * Builds test parameters similarly to {@link ESClientYamlSuiteTestCase#createParameters()},
     * replacing the body of index creation commands so that fields are {@code runtime_script}s
     * that load from {@code source} instead of their original type. Test configurations that
     * do are not modified to contain runtime fields are not returned as they are tested
     * elsewhere.
     */
    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        Map<String, ClientYamlTestSuite> suites = new HashMap<>();
        List<Object[]> result = new ArrayList<>();
        for (Object[] orig : ESClientYamlSuiteTestCase.createParameters()) {
            assert orig.length == 1;
            ClientYamlTestCandidate candidate = (ClientYamlTestCandidate) orig[0];
            ClientYamlTestSuite suite = suites.computeIfAbsent(candidate.getTestPath(), k -> modifiedSuite(candidate));
            if (suite == null) {
                // The setup section contains an unsupported option
                continue;
            }
            if (false == modifySection(candidate.getTestSection().getExecutableSections())) {
                // The test section contains an unsupported option
                continue;
            }
            ClientYamlTestSection modified = new ClientYamlTestSection(
                candidate.getTestSection().getLocation(),
                candidate.getTestSection().getName(),
                candidate.getTestSection().getSkipSection(),
                candidate.getTestSection().getExecutableSections()
            );
            result.add(new Object[] { new ClientYamlTestCandidate(suite, modified) });
        }
        return result;
    }

    /**
     * Modify the setup section to setup a dynamic template that replaces
     * field configurations with scripts that load from source
     * <strong>and</strong> replaces field configurations in {@code incides.create}
     * with scripts that load from source.
     */
    private static ClientYamlTestSuite modifiedSuite(ClientYamlTestCandidate candidate) {
        if (false == modifySection(candidate.getSetupSection().getExecutableSections())) {
            return null;
        }
        List<ExecutableSection> setup = new ArrayList<>(candidate.getSetupSection().getExecutableSections().size() + 1);
        setup.add(ADD_TEMPLATE);
        setup.addAll(candidate.getSetupSection().getExecutableSections());
        return new ClientYamlTestSuite(
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
     */
    private static boolean modifySection(List<ExecutableSection> executables) {
        for (ExecutableSection section : executables) {
            if (false == (section instanceof DoSection)) {
                continue;
            }
            DoSection doSection = (DoSection) section;
            if (false == doSection.getApiCallSection().getApi().equals("indices.create")) {
                continue;
            }
            for (Map<?, ?> body : doSection.getApiCallSection().getBodies()) {
                Object settings = body.get("settings");
                if (settings instanceof Map && ((Map<?, ?>) settings).containsKey("sort.field")) {
                    /*
                     * You can't sort the index on a runtime_keyword and it is
                     * hard to figure out if the sort was a runtime_keyword so
                     * let's just skip this test.
                     */
                    continue;
                }
                Object mappings = body.get("mappings");
                if (false == (mappings instanceof Map)) {
                    continue;
                }
                Object properties = ((Map<?, ?>) mappings).get("properties");
                if (false == (properties instanceof Map)) {
                    continue;
                }
                for (Map.Entry<?, ?> property : ((Map<?, ?>) properties).entrySet()) {
                    if (false == property.getValue() instanceof Map) {
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
                        // If doc_values is false we can't emulate with scripts. `null` and `true` are fine.
                        continue;
                    }
                    if ("false".equals(Objects.toString(propertyMap.get("index")))) {
                        // If index is false we can't emulate with scripts
                        continue;
                    }
                    if ("true".equals(Objects.toString(propertyMap.get("store")))) {
                        // If store is true we can't emulate with scripts
                        continue;
                    }
                    if (propertyMap.containsKey("ignore_above")) {
                        // Scripts don't support ignore_above so we skip those fields
                        continue;
                    }
                    if (propertyMap.containsKey("ignore_malformed")) {
                        // Our source reading script doesn't emulate ignore_malformed
                        continue;
                    }
                    String toLoad = painlessToLoadFromSource(name, type);
                    if (toLoad == null) {
                        continue;
                    }
                    propertyMap.put("type", "runtime_script");
                    propertyMap.put("runtime_type", type);
                    propertyMap.put("script", toLoad);
                    propertyMap.remove("store");
                    propertyMap.remove("index");
                    propertyMap.remove("doc_values");
                }
            }
        }
        return true;
    }

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
        Map.entry(BooleanFieldMapper.CONTENT_TYPE, "emitValue(parse(value));"),
        Map.entry(DateFieldMapper.CONTENT_TYPE, "emitValue(parse(value.toString()));"),
        Map.entry(
            NumberType.DOUBLE.typeName(),
            "emitValue(value instanceof Number ? ((Number) value).doubleValue() : Double.parseDouble(value.toString()));"
        ),
        Map.entry(KeywordFieldMapper.CONTENT_TYPE, "emitValue(value.toString());"),
        Map.entry(IpFieldMapper.CONTENT_TYPE, "emitValue(value.toString());"),
        Map.entry(
            NumberType.LONG.typeName(),
            "emitValue(value instanceof Number ? ((Number) value).longValue() : Long.parseLong(value.toString()));"
        )
    );

    private static final ExecutableSection ADD_TEMPLATE = new ExecutableSection() {
        @Override
        public XContentLocation getLocation() {
            return new XContentLocation(-1, -1);
        }

        @Override
        public void execute(ClientYamlTestExecutionContext executionContext) throws IOException {
            Map<String, String> params = Map.of("name", "convert_to_source_only", "create", "true");
            List<Map<String, Object>> dynamicTemplates = new ArrayList<>();
            for (String type : PAINLESS_TO_EMIT.keySet()) {
                if (type.equals("ip")) {
                    // There isn't a dynamic template to pick up ips. They'll just look like strings.
                    continue;
                }
                Map<String, Object> mapping = Map.ofEntries(
                    Map.entry("type", "runtime_script"),
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
            List<Map<String, Object>> bodies = List.of(
                Map.ofEntries(
                    Map.entry("index_patterns", "*"),
                    Map.entry("priority", Integer.MAX_VALUE - 1),
                    Map.entry("template", Map.of("settings", Map.of(), "mappings", Map.of("dynamic_templates", dynamicTemplates)))
                )
            );
            ClientYamlTestResponse response = executionContext.callApi("indices.put_index_template", params, bodies, Map.of());
            assertThat(response.getStatusCode(), equalTo(200));
            // There are probably some warning about overlapping templates. Ignore them.
        }
    };
}
