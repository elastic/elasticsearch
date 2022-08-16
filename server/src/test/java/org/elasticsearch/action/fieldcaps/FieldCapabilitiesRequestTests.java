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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xcontent.ObjectParser.fromList;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

public class FieldCapabilitiesRequestTests extends AbstractWireSerializingTestCase<FieldCapabilitiesRequest> {

    @Override
    protected FieldCapabilitiesRequest createTestInstance() {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
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
        if (randomBoolean()) {
            request.filters("-nested");
        }
        if (randomBoolean()) {
            request.types(randomAlphaOfLength(5));
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
            String[] fields = ArrayUtils.concat(request.fields(), new String[] { randomAlphaOfLength(10) });
            request.fields(fields);
        });
        mutators.add(request -> {
            String[] indices = ArrayUtils.concat(instance.indices(), generateRandomStringArray(5, 10, false, false));
            request.indices(indices);
        });
        mutators.add(request -> {
            IndicesOptions indicesOptions = randomValueOtherThan(
                request.indicesOptions(),
                () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
            );
            request.indicesOptions(indicesOptions);
        });
        mutators.add(request -> request.setMergeResults(request.isMergeResults() == false));
        mutators.add(request -> request.includeUnmapped(request.includeUnmapped() == false));
        mutators.add(request -> request.nowInMillis(request.nowInMillis() != null ? request.nowInMillis() + 1 : 1L));
        mutators.add(
            request -> request.indexFilter(request.indexFilter() != null ? request.indexFilter().boost(2) : QueryBuilders.matchAllQuery())
        );
        mutators.add(request -> request.runtimeFields(Collections.singletonMap("other_key", "other_value")));
        mutators.add(request -> request.filters(request.filters().length == 0 ? new String[] { "-metadata" } : Strings.EMPTY_ARRAY));
        mutators.add(request -> request.types(request.types().length == 0 ? new String[] { "keyword" } : Strings.EMPTY_ARRAY));

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
        assertEquals(("""
            {  "index_filter": {
                "term": {
                  "field": {
                    "value": "value"
                  }
                }
              },
              "runtime_mappings": {
                "day_of_week": {
                  "type": "keyword"
                }
              }
            }""").replaceAll("\\s+", ""), xContent);
    }

    public void testFromXContent() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"fields\" : [\"FOO\"] }");
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
        ObjectParser<FieldCapabilitiesRequest, Void> PARSER = new ObjectParser<>("field_caps_request");
        PARSER.declareStringArray(fromList(String.class, FieldCapabilitiesRequest::fields), new ParseField("fields"));

        PARSER.parse(parser, request, null);

        assertArrayEquals(request.fields(), new String[] { "FOO" });

    }

    public void testValidation() {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest().indices("index2");
        ActionRequestValidationException exception = request.validate();
        assertNotNull(exception);
    }

    public void testGetDescription() {
        final FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
        assertThat(request.getDescription(), equalTo("indices[], fields[], filters[], types[]"));

        request.fields("a", "b");
        assertThat(
            request.getDescription(),
            anyOf(equalTo("indices[], fields[a,b], filters[], types[]"), equalTo("indices[], fields[b,a], filters[], types[]"))
        );

        request.indices("x", "y", "z");
        request.fields("a");
        assertThat(request.getDescription(), equalTo("indices[x,y,z], fields[a], filters[], types[]"));

        request.filters("-metadata", "-multifields");
        assertThat(request.getDescription(), endsWith("filters[-metadata,-multifields], types[]"));

        final String[] lots = new String[between(1024, 2048)];
        for (int i = 0; i < lots.length; i++) {
            lots[i] = "s" + i;
        }

        request.indices("x", "y", "z");
        request.fields(lots);
        request.filters(Strings.EMPTY_ARRAY);
        assertThat(
            request.getDescription(),
            allOf(
                startsWith("indices[x,y,z], fields["),
                containsString("..."),
                containsString(lots.length + " in total"),
                containsString("omitted")
            )
        );
        assertThat(
            request.getDescription().length(),
            lessThanOrEqualTo(1024 + ("indices[x,y,z], fields[" + "s9999,... (9999 in total, 9999 omitted)], filters[], types[]").length())
        );

        request.fields("a");
        request.indices(lots);
        assertThat(
            request.getDescription(),
            allOf(
                startsWith("indices[s0,s1,s2,s3"),
                containsString("..."),
                containsString(lots.length + " in total"),
                containsString("omitted"),
                endsWith("], fields[a], filters[], types[]")
            )
        );
        assertThat(
            request.getDescription().length(),
            lessThanOrEqualTo(1024 + ("indices[" + "s9999,... (9999 in total, 9999 omitted)], fields[a], filters[], types[]").length())
        );

        final FieldCapabilitiesRequest randomRequest = createTestInstance();
        final String description = randomRequest.getDescription();
        for (String index : randomRequest.indices()) {
            assertThat(description, containsString(index));
        }
        for (String field : randomRequest.fields()) {
            assertThat(description, containsString(field));
        }
    }

}
