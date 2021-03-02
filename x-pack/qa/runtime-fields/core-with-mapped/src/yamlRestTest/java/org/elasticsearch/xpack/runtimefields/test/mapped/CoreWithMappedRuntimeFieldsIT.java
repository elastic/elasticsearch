/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.test.mapped;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.section.ApiCallSection;
import org.elasticsearch.xpack.runtimefields.test.CoreTestTranslater;

import java.util.Map;

/**
 * Runs elasticsearch's core rest tests replacing all field mappings with runtime fields
 * that load from {@code _source}. Tests that configure the field in a way that are not
 * supported by runtime fields are skipped.
 */
public class CoreWithMappedRuntimeFieldsIT extends ESClientYamlSuiteTestCase {
    public CoreWithMappedRuntimeFieldsIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return new MappingRuntimeFieldTranslater().parameters();
    }

    private static class MappingRuntimeFieldTranslater extends CoreTestTranslater {
        @Override
        protected Map<String, Object> dynamicTemplateFor() {
            return dynamicTemplateToAddRuntimeFields();
        }

        @Override
        protected Suite suite(ClientYamlTestCandidate candidate) {
            return new Suite(candidate) {
                @Override
                protected boolean modifyMappingProperties(String index, Map<String, Object> properties, Map<String, Object> runtimeFields) {
                    return runtimeifyMappingProperties(properties, runtimeFields);
                }

                @Override
                protected boolean modifySearch(ApiCallSection search) {
                    // We don't need to modify the search request if the mappings are in the index
                    return true;
                }

                @Override
                protected boolean handleIndex(IndexRequest index) {
                    // We don't need to scrape anything out of the index requests.
                    return true;
                }
            };
        }
    }
}
