/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.datageneration.datasource.DefaultMappingParametersHandler;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TextFieldWithParentBlockLoaderTests extends BlockLoaderTestCase {
    public TextFieldWithParentBlockLoaderTests(Params params) {
        // keyword because we need a keyword parent field
        super(FieldType.KEYWORD.toString(), List.of(new DataSourceHandler() {
            @Override
            public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
                assert request.fieldType().equals(FieldType.KEYWORD.toString());

                // We need to force multi field generation
                return new DataSourceResponse.LeafMappingParametersGenerator(() -> {
                    var defaultSupplier = DefaultMappingParametersHandler.keywordMapping(
                        request,
                        DefaultMappingParametersHandler.commonMappingParameters()
                    );
                    var mapping = defaultSupplier.get();
                    // we don't need this here
                    mapping.remove("copy_to");

                    var textMultiFieldMappingSupplier = DefaultMappingParametersHandler.textMapping(request, new HashMap<>());
                    var textMultiFieldMapping = textMultiFieldMappingSupplier.get();
                    textMultiFieldMapping.put("type", "text");
                    textMultiFieldMapping.remove("fields");

                    mapping.put("fields", Map.of("txt", textMultiFieldMapping));

                    return mapping;
                });
            }
        }), params);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        assert fieldMapping.containsKey("fields");

        Object normalizer = fieldMapping.get("normalizer");
        boolean docValues = hasDocValues(fieldMapping, true);
        boolean store = fieldMapping.getOrDefault("store", false).equals(true);

        if (normalizer == null && (docValues || store)) {
            // we are using block loader of the parent field
            return KeywordFieldBlockLoaderTests.expectedValue(fieldMapping, value, params, testContext);
        }

        // we are using block loader of the text field itself
        var textFieldMapping = (Map<String, Object>) ((Map<String, Object>) fieldMapping.get("fields")).get("txt");
        return TextFieldBlockLoaderTests.expectedValue(textFieldMapping, value, params, testContext);
    }

    @Override
    protected String blockLoaderFieldName(String originalName) {
        return originalName + ".txt";
    }
}
