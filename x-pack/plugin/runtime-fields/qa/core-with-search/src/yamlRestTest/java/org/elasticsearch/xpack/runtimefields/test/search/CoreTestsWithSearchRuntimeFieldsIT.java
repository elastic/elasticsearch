/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.test.search;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.section.ApiCallSection;
import org.elasticsearch.xpack.runtimefields.test.CoreTestTranslater;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;

public class CoreTestsWithSearchRuntimeFieldsIT extends ESClientYamlSuiteTestCase {
    public CoreTestsWithSearchRuntimeFieldsIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return new SearchRequestRuntimeFieldTranslater().parameters();
    }

    /**
     * Builds test parameters similarly to {@link ESClientYamlSuiteTestCase#createParameters()},
     * replacing all fields with runtime fields that load from {@code _source} if possible. Tests
     * that configure the field in a way that are not supported by runtime fields are skipped.
     */
    private static class SearchRequestRuntimeFieldTranslater extends CoreTestTranslater {
        @Override
        protected List<Map<String, Object>> dynamicTemplates() {
            return dynamicTemplatesToDisableAllFields();
        }

        @Override
        protected Suite suite(ClientYamlTestCandidate candidate) {
            return new Suite(candidate) {
                private final Map<String, Map<?, ?>> runtimeMappings = new HashMap<>();;

                @Override
                protected boolean modifyMappingProperties(String index, Map<?, ?> properties) {
                    Map<?, ?> indexRuntimeMappings = new HashMap<>(properties);
                    if (false == runtimeifyMappingProperties(properties)) {
                        return false;
                    }
                    runtimeMappings.put(index, indexRuntimeMappings);
                    return true;
                }

                @Override
                protected void modifyMapping(Map<String, Object> mapping) {
                    mapping.clear();
                    mapping.put("dynamic", false);
                }

                @Override
                protected boolean modifySearch(ApiCallSection search) {
                    assertThat(search.getBodies(), hasSize(1));
                    Map<String, Object> body = search.getBodies().get(0);
                    Object index = body.get("index");
                    if (index == null) {
                        // NOCOMMIT hack together a mapping based on merging everything
                        return false;
                    }
                    Map<?, ?> runtimeMapping = runtimeMappings.get(index);
                    if (runtimeMapping == null) {
                        return false;
                    }
                    search.getBodies().get(0).put("runtime_mappings", runtimeMapping);
                    return true;
                }
            };
        }
    }
}
