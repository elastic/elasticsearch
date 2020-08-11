/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
        /*
         * Map of "setup"s that we've seen - from path to whether or
         * not we the setup was modified to include a runtime_script
         */
        Map<String, Boolean> seenSetups = new HashMap<>();
        List<Object[]> result = new ArrayList<>();
        for (Object[] orig : ESClientYamlSuiteTestCase.createParameters()) {
            assert orig.length == 1;
            ClientYamlTestCandidate candidate = (ClientYamlTestCandidate) orig[0];
            boolean modifiedSetup = seenSetups.computeIfAbsent(
                candidate.getName(),
                k -> modifySection(candidate.getSuitePath() + "/setup", candidate.getSetupSection().getExecutableSections())
            );
            boolean modifiedTest = modifySection(candidate.getTestPath(), candidate.getTestSection().getExecutableSections());
            if (modifiedSetup || modifiedTest) {
                result.add(new Object[] { candidate });
            }
        }
        return result;
    }

    /**
     * Replace property configuration in {@code indices.create} with scripts
     * that load from the source.
     * @return {@code true} if any fields were rewritten into runtime_scripts, {@code false} otherwise.
     */
    private static boolean modifySection(String sectionName, List<ExecutableSection> executables) {
        boolean include = false;
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
                    include = true;
                }
            }
        }
        return include;
    }

    private static String painlessToLoadFromSource(String name, String type) {
        String emit = PAINLESS_TO_EMIT.get(type);
        if (emit == null) {
            return null;
        }
        StringBuilder b = new StringBuilder();
        b.append("def v = source['").append(name).append("'];\n");
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
        // TODO implement dates against the parser
        Map.entry(
            NumberType.DOUBLE.typeName(),
            "value(value instanceof Number ? ((Number) value).doubleValue() : Double.parseDouble(value.toString()));"
        ),
        Map.entry(KeywordFieldMapper.CONTENT_TYPE, "value(value.toString());"),
        Map.entry(IpFieldMapper.CONTENT_TYPE, "stringValue(value.toString());"),
        Map.entry(
            NumberType.LONG.typeName(),
            "value(value instanceof Number ? ((Number) value).longValue() : Long.parseLong(value.toString()));"
        )
    );

}
