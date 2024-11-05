/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.FieldFetcher;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

public class PlaceHolderFieldMapperTests extends MapperServiceTestCase {

    // checks that parameters of unknown field types are preserved on legacy indices
    public void testPreserveParams() throws Exception {
        XContentBuilder mapping = mapping(b -> {
            b.startObject("myfield");
            b.field("type", "unknown");
            b.field("someparam", "value");
            b.endObject();
        });
        MapperService service = createMapperService(IndexVersion.fromId(5000099), Settings.EMPTY, () -> false, mapping);
        assertThat(service.fieldType("myfield"), instanceOf(PlaceHolderFieldMapper.PlaceHolderFieldType.class));
        assertEquals(Strings.toString(mapping), Strings.toString(service.documentMapper().mapping()));

        // check that field can be updated
        mapping = mapping(b -> {
            b.startObject("myfield");
            b.field("type", "unknown");
            b.field("someparam", "other");
            b.endObject();
        });
        merge(service, mapping);
        assertThat(service.fieldType("myfield"), instanceOf(PlaceHolderFieldMapper.PlaceHolderFieldType.class));
        assertEquals(Strings.toString(mapping), Strings.toString(service.documentMapper().mapping()));
    }

    public void testFetchValue() throws Exception {
        MapperService mapperService = createMapperService(IndexVersion.fromId(5000099), fieldMapping(b -> b.field("type", "unknown")));
        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(
                createMapperService(fieldMapping(b -> b.field("type", "keyword"))).documentMapper()
                    .parse(source(b -> b.field("field", "value")))
                    .rootDoc()
            );
        }, iw -> {
            SearchLookup lookup = new SearchLookup(
                mapperService::fieldType,
                fieldDataLookup(mapperService),
                SourceProvider.fromStoredFields()
            );
            SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService);
            FieldFetcher fieldFetcher = FieldFetcher.create(
                searchExecutionContext,
                Collections.singletonList(new FieldAndFormat("field", null))
            );
            IndexSearcher searcher = newSearcher(iw);
            LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
            Source source = lookup.getSource(context, 0);
            Map<String, DocumentField> fields = fieldFetcher.fetch(source, 0);
            assertEquals(1, fields.size());
            assertEquals(List.of("value"), fields.get("field").getValues());
        });
    }
}
