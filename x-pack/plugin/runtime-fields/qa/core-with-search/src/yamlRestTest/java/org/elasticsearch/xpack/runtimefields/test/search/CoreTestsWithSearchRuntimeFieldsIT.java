/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.test.search;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.section.ApiCallSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.elasticsearch.xpack.runtimefields.test.CoreTestTranslater;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static org.hamcrest.Matchers.hasSize;

/**
 * Runs elasticsearch's core rest tests disabling all mappings and replacing them
 * with runtime fields defined on the search request that load from {@code _source}. Tests
 * that configure the field in a way that are not supported by runtime fields are skipped.
 */
public class CoreTestsWithSearchRuntimeFieldsIT extends ESClientYamlSuiteTestCase {
    public CoreTestsWithSearchRuntimeFieldsIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected boolean randomizeContentType() { // NOCOMMIT remove me
        return false;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return new SearchRequestRuntimeFieldTranslater().parameters();
    }

    private static class SearchRequestRuntimeFieldTranslater extends CoreTestTranslater {
        @Override
        protected Map<String, Object> indexTemplate() {
            return indexTemplateToDisableAllFields();
        }

        @Override
        protected Suite suite(ClientYamlTestCandidate candidate) {
            return new Suite(candidate) {
                private Map<String, Map<?, ?>> runtimeMappingsAfterSetup;
                private Map<String, Map<?, ?>> runtimeMappings;

                @Override
                public boolean modifySections(List<ExecutableSection> executables) {
                    if (runtimeMappingsAfterSetup == null) {
                        if (candidate.toString().contains("50_filter")) {
                            LogManager.getLogger().error("init");
                        }
                        // We're modifying the setup section
                        runtimeMappings = new HashMap<>();
                        if (false == super.modifySections(executables)) {
                            return false;
                        }
                        runtimeMappingsAfterSetup = unmodifiableMap(runtimeMappings);
                        runtimeMappings = null;
                        return true;
                    }
                    runtimeMappings = new HashMap<>(runtimeMappingsAfterSetup);
                    return super.modifySections(executables);
                }

                @Override
                protected boolean modifyMapping(String index, Map<String, Object> mapping) {
                    Object properties = mapping.get("properties");
                    if (properties == null || false == (properties instanceof Map)) {
                        return true;
                    }
                    mapping.put("dynamic", false);
                    @SuppressWarnings("unchecked")
                    Map<String, Object> propertiesMap = (Map<String, Object>) properties;
                    Map<String, Object> untouchedMapping = new HashMap<>();
                    Map<String, Map<String, Object>> runtimeMapping = new HashMap<>();
                    if (false == runtimeifyMappingProperties(propertiesMap, untouchedMapping, runtimeMapping)) {
                        return false;
                    }
                    mapping.put("properties", untouchedMapping);
                    if (candidate.toString().contains("50_filter")) {
                        LogManager.getLogger().error("untouched " + untouchedMapping);
                        LogManager.getLogger().error("runtime " + runtimeMapping);
                    }
                    runtimeMappings.put(index, runtimeMapping);
                    return true;
                }

                @Override
                protected boolean modifySearch(ApiCallSection search) {
                    if (candidate.toString().contains("50_filter")) {
                        LogManager.getLogger().error("toString " + candidate);
                        LogManager.getLogger().error("runtimeMappingsAfterSetup " + runtimeMappingsAfterSetup);
                        LogManager.getLogger().error("runtimeMappings " + runtimeMappings);
                    }
                    Map<String, Object> body;
                    if (search.getBodies().isEmpty()) {
                        body = new HashMap<>();
                        search.addBody(body);
                    } else {
                        body = search.getBodies().get(0);
                        assertThat(search.getBodies(), hasSize(1));
                    }
                    Object index = body.get("index");
                    Map<?, ?> runtimeMapping = runtimeMappings(index);
                    if (runtimeMapping == null) {
                        return false;
                    }
                    search.getBodies().get(0).put("runtime_mappings", runtimeMapping);
                    return true;
                }

                private Map<?, ?> runtimeMappings(Object index) {
                    if (index != null) {
                        return runtimeMappings.get(index);
                    }
                    // No mapping index specified in the search, if there is just one index we can just us it
                    if (runtimeMappings.size() == 1) {
                        return runtimeMappings.values().iterator().next();
                    }
                    return null;
                }
            };
        }
    }
}
