/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.ml.job.results.ReservedFieldNames;
import org.elasticsearch.xpack.ml.job.results.Result;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class ElasticsearchMappingsTests extends ESTestCase {

    public void testReservedFields() throws Exception {
        Set<String> overridden = new HashSet<>();

        // These are not reserved because they're Elasticsearch keywords, not
        // field names
        overridden.add(ElasticsearchMappings.ANALYZER);
        overridden.add(ElasticsearchMappings.COPY_TO);
        overridden.add(ElasticsearchMappings.DYNAMIC);
        overridden.add(ElasticsearchMappings.ENABLED);
        overridden.add(ElasticsearchMappings.NESTED);
        overridden.add(ElasticsearchMappings.PROPERTIES);
        overridden.add(ElasticsearchMappings.TYPE);
        overridden.add(ElasticsearchMappings.WHITESPACE);

        // These are not reserved because they're data types, not field names
        overridden.add(Result.TYPE.getPreferredName());
        overridden.add(DataCounts.TYPE.getPreferredName());
        overridden.add(CategoryDefinition.TYPE.getPreferredName());
        overridden.add(ModelSizeStats.RESULT_TYPE_FIELD.getPreferredName());
        overridden.add(ModelSnapshot.TYPE.getPreferredName());
        overridden.add(Quantiles.TYPE.getPreferredName());

        Set<String> expected = collectResultsDocFieldNames();

        expected.removeAll(overridden);

        if (ReservedFieldNames.RESERVED_FIELD_NAMES.size() != expected.size()) {
            Set<String> diff = new HashSet<>(ReservedFieldNames.RESERVED_FIELD_NAMES);
            diff.removeAll(expected);
            StringBuilder errorMessage = new StringBuilder("Fields in ReservedFieldNames but not in expected: ").append(diff);

            diff = new HashSet<>(expected);
            diff.removeAll(ReservedFieldNames.RESERVED_FIELD_NAMES);
            errorMessage.append("\nFields in expected but not in ReservedFieldNames: ").append(diff);
            fail(errorMessage.toString());
        }
        assertEquals(ReservedFieldNames.RESERVED_FIELD_NAMES.size(), expected.size());

        for (String s : expected) {
            // By comparing like this the failure messages say which string is missing
            String reserved = ReservedFieldNames.RESERVED_FIELD_NAMES.contains(s) ? s : null;
            assertEquals(s, reserved);
        }
    }

    @SuppressWarnings("unchecked")
    public void testTermFieldMapping() throws IOException {

        XContentBuilder builder = ElasticsearchMappings.termFieldsMapping(null, Arrays.asList("apple", "strawberry",
                AnomalyRecord.BUCKET_SPAN.getPreferredName()));

        XContentParser parser = createParser(builder);
        Map<String, Object> properties = (Map<String, Object>) parser.map().get(ElasticsearchMappings.PROPERTIES);

        Map<String, Object> instanceMapping = (Map<String, Object>) properties.get("apple");
        assertNotNull(instanceMapping);
        String dataType = (String)instanceMapping.get(ElasticsearchMappings.TYPE);
        assertEquals(ElasticsearchMappings.KEYWORD, dataType);

        instanceMapping = (Map<String, Object>) properties.get("strawberry");
        assertNotNull(instanceMapping);
        dataType = (String)instanceMapping.get(ElasticsearchMappings.TYPE);
        assertEquals(ElasticsearchMappings.KEYWORD, dataType);

        // check no mapping for the reserved field
        instanceMapping = (Map<String, Object>) properties.get(AnomalyRecord.BUCKET_SPAN.getPreferredName());
        assertNull(instanceMapping);
    }

    private Set<String> collectResultsDocFieldNames() throws IOException {
        // Only the mappings for the results index should be added below.  Do NOT add mappings for other indexes here.

        XContentBuilder builder = ElasticsearchMappings.docMapping();
        BufferedInputStream inputStream =
                new BufferedInputStream(new ByteArrayInputStream(builder.string().getBytes(StandardCharsets.UTF_8)));
        JsonParser parser = new JsonFactory().createParser(inputStream);
        Set<String> fieldNames = new HashSet<>();
        boolean isAfterPropertiesStart = false;
        try {
            JsonToken token = parser.nextToken();
            while (token != null) {
                switch (token) {
                    case START_OBJECT:
                        break;
                    case FIELD_NAME:
                        String fieldName = parser.getCurrentName();
                        if (isAfterPropertiesStart) {
                            fieldNames.add(fieldName);
                        } else {
                            if (ElasticsearchMappings.PROPERTIES.equals(fieldName)) {
                                isAfterPropertiesStart = true;
                            }
                        }
                        break;
                    default:
                        break;
                }
                token = parser.nextToken();
            }
        } catch (JsonParseException e) {
            fail("Cannot parse JSON: " + e);
        }

        return fieldNames;
    }
}
