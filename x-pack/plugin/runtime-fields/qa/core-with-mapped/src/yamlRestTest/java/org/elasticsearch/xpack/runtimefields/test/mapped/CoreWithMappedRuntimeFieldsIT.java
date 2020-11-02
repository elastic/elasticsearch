/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.test.mapped;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.section.ApiCallSection;
import org.elasticsearch.xpack.runtimefields.test.CoreTestTranslater;

import java.util.List;
import java.util.Map;

public class CoreWithMappedRuntimeFieldsIT extends ESClientYamlSuiteTestCase {
    public CoreWithMappedRuntimeFieldsIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return new MappingRuntimeFieldTranslater().parameters();
    }

    /**
     * Builds test parameters similarly to {@link ESClientYamlSuiteTestCase#createParameters()},
     * replacing all fields with runtime fields that load from {@code _source} if possible. Tests
     * that configure the field in a way that are not supported by runtime fields are skipped.
     */
    private static class MappingRuntimeFieldTranslater extends CoreTestTranslater {
        @Override
        protected List<Map<String, Object>> dynamicTemplates() {
            return dynamicTemplatesToAddRuntimeFieldsToMappings();
        }

        @Override
        protected Suite suite(ClientYamlTestCandidate candidate) {
            return new Suite(candidate) {
                @Override
                protected boolean modifyMappingProperties(String index, Map<?, ?> properties) {
                    return runtimeifyMappingProperties(properties);
                }

                @Override
                protected void modifyMapping(Map<String, Object> mapping) {
                    // The top level mapping is fine for runtime fields defined in the mapping
                }

                @Override
                protected boolean modifySearch(ApiCallSection search) {
                    // We don't need to modify the search request if the mappings are in the index
                    return true;
                }
            };
        }

    }
}
