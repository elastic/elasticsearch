/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;

public class FieldCapabilitiesRequestTests extends AbstractWireSerializingTestCase<FieldCapabilitiesRequest> {

    @Override
    protected FieldCapabilitiesRequest createTestInstance() {
        FieldCapabilitiesRequest request =  new FieldCapabilitiesRequest();
        int size = randomIntBetween(1, 20);
        String[] randomFields = new String[size];
        for (int i = 0; i < size; i++) {
            randomFields[i] = randomAlphaOfLengthBetween(5, 10);
        }

        size = randomIntBetween(0, 20);
        String[] randomIndices = new String[size];
        for (int i = 0; i < size; i++) {
            randomIndices[i] = randomAlphaOfLengthBetween(5, 10);
        }
        request.fields(randomFields);
        request.indices(randomIndices);
        if (randomBoolean()) {
            request.indicesOptions(randomBoolean() ? IndicesOptions.strictExpand() : IndicesOptions.lenientExpandOpen());
        }
        request.includeUnmapped(randomBoolean());
        if (randomBoolean()) {
            request.nowInMillis(randomLong());
        }
        if (randomBoolean()) {
            request.indexFilter(QueryBuilders.termQuery("field", randomAlphaOfLength(5)));
        }
        if (randomBoolean()) {
            request.runtimeFields(Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        }
        return request;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected Writeable.Reader<FieldCapabilitiesRequest> instanceReader() {
        return FieldCapabilitiesRequest::new;
    }

    @Override
    protected FieldCapabilitiesRequest mutateInstance(FieldCapabilitiesRequest instance) throws IOException {
        List<Consumer<FieldCapabilitiesRequest>> mutators = new ArrayList<>();
        mutators.add(request -> {
            String[] fields = ArrayUtils.concat(request.fields(), new String[] {randomAlphaOfLength(10)});
            request.fields(fields);
        });
        mutators.add(request -> {
            String[] indices = ArrayUtils.concat(instance.indices(), generateRandomStringArray(5, 10, false, false));
            request.indices(indices);
        });
        mutators.add(request -> {
            IndicesOptions indicesOptions = randomValueOtherThan(request.indicesOptions(),
                () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
            request.indicesOptions(indicesOptions);
        });
        mutators.add(request -> request.setMergeResults(request.isMergeResults() == false));
        mutators.add(request -> request.includeUnmapped(request.includeUnmapped() == false));
        mutators.add(request -> request.nowInMillis(request.nowInMillis() != null ? request.nowInMillis() + 1 : 1L));
        mutators.add(
            request -> request.indexFilter(request.indexFilter() != null ? request.indexFilter().boost(2) : QueryBuilders.matchAllQuery())
        );
        mutators.add(request -> request.runtimeFields(Collections.singletonMap("other_key", "other_value")));

        FieldCapabilitiesRequest mutatedInstance = copyInstance(instance);
        Consumer<FieldCapabilitiesRequest> mutator = randomFrom(mutators);
        mutator.accept(mutatedInstance);
        return mutatedInstance;
    }

    public void testToXContent() throws IOException {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
        request.indexFilter(QueryBuilders.termQuery("field", "value"));
        request.runtimeFields(singletonMap("day_of_week", singletonMap("type", "keyword")));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        String xContent = BytesReference.bytes(request.toXContent(builder, ToXContent.EMPTY_PARAMS)).utf8ToString();
        assertEquals(
            ("{"
                + "  \"index_filter\": {\n"
                + "    \"term\": {\n"
                + "      \"field\": {\n"
                + "        \"value\": \"value\",\n"
                + "        \"boost\": 1.0\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"runtime_mappings\": {\n"
                + "    \"day_of_week\": {\n"
                + "      \"type\": \"keyword\"\n"
                + "    }\n"
                + "  }\n"
                + "}").replaceAll("\\s+", ""),
            xContent
        );
    }

    public void testValidation() {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest()
            .indices("index2");
        ActionRequestValidationException exception = request.validate();
        assertNotNull(exception);
    }
}
