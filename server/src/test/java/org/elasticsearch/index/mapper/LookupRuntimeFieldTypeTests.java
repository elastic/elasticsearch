/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.LookupField;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class LookupRuntimeFieldTypeTests extends MapperServiceTestCase {

    public void testFetchValues() throws IOException {
        String mapping = """
            {
              "_doc": {
                "properties" : {
                  "foo" : {
                    "type" : "keyword"
                  }
                },
                "runtime": {
                    "foo_lookup_field": {
                        "type": "lookup",
                        "target_index": "my_index",
                        "input_field": "foo",
                        "target_field": "term_field_foo",
                        "fetch_fields": [
                            "remote_field_*",
                            {"field": "created", "format": "YYYY-dd-MM"}
                        ]
                    }
                }
              }
            }
            """;
        var mapperService = createMapperService(mapping);
        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("foo", List.of("f1", "f2")).endObject();
        SourceLookup sourceLookup = new SourceLookup(new SourceLookup.BytesSourceProvider(BytesReference.bytes(source)));
        MappedFieldType fieldType = mapperService.fieldType("foo_lookup_field");
        ValueFetcher valueFetcher = fieldType.valueFetcher(createSearchExecutionContext(mapperService), null);
        DocumentField doc = valueFetcher.fetchDocumentField("foo_lookup_field", sourceLookup);
        assertNotNull(doc);
        assertThat(doc.getName(), equalTo("foo_lookup_field"));
        assertThat(doc.getValues(), empty());
        assertThat(doc.getIgnoredValues(), empty());
        assertThat(
            doc.getLookupFields(),
            contains(
                new LookupField(
                    "my_index",
                    new TermQueryBuilder("term_field_foo", "f1"),
                    List.of(new FieldAndFormat("remote_field_*", null), new FieldAndFormat("created", "YYYY-dd-MM")),
                    1
                ),
                new LookupField(
                    "my_index",
                    new TermQueryBuilder("term_field_foo", "f2"),
                    List.of(new FieldAndFormat("remote_field_*", null), new FieldAndFormat("created", "YYYY-dd-MM")),
                    1
                )
            )
        );
    }

    public void testEmptyInputField() throws IOException {
        String mapping = """
            {
              "_doc": {
                "properties" : {
                  "foo" : {
                    "type" : "keyword"
                  }
                },
                "runtime": {
                    "foo_lookup_field": {
                        "type": "lookup",
                        "target_index": "my_index",
                        "input_field": "foo",
                        "target_field": "term_field_foo",
                        "fetch_fields": ["remote_field_*"]
                    }
                }
              }
            }
            """;
        var mapperService = createMapperService(mapping);
        XContentBuilder source = XContentFactory.jsonBuilder();
        source.startObject();
        if (randomBoolean()) {
            source.field("foo", List.of());
        }
        source.endObject();
        SourceLookup sourceLookup = new SourceLookup(new SourceLookup.BytesSourceProvider(BytesReference.bytes(source)));
        MappedFieldType fieldType = mapperService.fieldType("foo_lookup_field");
        ValueFetcher valueFetcher = fieldType.valueFetcher(createSearchExecutionContext(mapperService), null);
        DocumentField doc = valueFetcher.fetchDocumentField("foo_lookup_field", sourceLookup);
        assertNull(doc);
    }

    public void testInputFieldDoesNotExist() throws IOException {
        String mapping = """
            {
              "_doc": {
                "runtime": {
                    "foo_lookup_field": {
                        "type": "lookup",
                        "target_index": "my_index",
                        "input_field": "barbaz",
                        "target_field": "term_field_foo",
                        "fetch_fields": ["field-1", "field-2"]
                    }
                }
              }
            }
            """;
        var mapperService = createMapperService(mapping);
        MappedFieldType fieldType = mapperService.fieldType("foo_lookup_field");
        // fails if unmapped_fields is not
        QueryShardException error = expectThrows(QueryShardException.class, () -> {
            SearchExecutionContext context = createSearchExecutionContext(mapperService);
            context.setAllowUnmappedFields(randomBoolean());
            fieldType.valueFetcher(context, null);
        });
        assertThat(error.getMessage(), containsString("No field mapping can be found for the field with name [barbaz]"));
    }
}
