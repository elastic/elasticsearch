/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public abstract class NativeArrayIntegrationTestCase extends ESSingleNodeTestCase {

    public void testSynthesizeEmptyArray() throws Exception {
        var arrayValues = new Object[][] { new Object[] {} };
        verifySyntheticArray(arrayValues);
    }

    public void testSynthesizeArrayRandom() throws Exception {
        var arrayValues = new Object[randomInt(64)];
        for (int j = 0; j < arrayValues.length; j++) {
            arrayValues[j] = getRandomValue();
        }
        verifySyntheticArray(new Object[][] { arrayValues });
    }

    public void testSynthesizeArrayInObjectFieldRandom() throws Exception {
        List<Object[]> documents = new ArrayList<>();
        int numDocs = randomIntBetween(8, 256);
        for (int i = 0; i < numDocs; i++) {
            Object[] document = new Object[randomInt(64)];
            for (int j = 0; j < document.length; j++) {
                document[j] = getRandomValue();
            }
            documents.add(document);
        }
        verifySyntheticArrayInObject(documents);
    }

    public void testSynthesizeArrayRandomIgnoresMalformed() throws Exception {
        assumeTrue("supports ignore_malformed", getMalformedValue() != null);
        int numDocs = randomIntBetween(8, 256);
        List<XContentBuilder> expectedDocuments = new ArrayList<>(numDocs);
        List<XContentBuilder> inputDocuments = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            Object[] values = new Object[randomInt(64)];
            Object[] malformed = new Object[randomInt(64)];
            for (int j = 0; j < values.length; j++) {
                values[j] = getRandomValue();
            }
            for (int j = 0; j < malformed.length; j++) {
                malformed[j] = getMalformedValue();
            }

            var expectedDocument = jsonBuilder().startObject();
            var inputDocument = jsonBuilder().startObject();

            boolean expectedIsArray = values.length != 0 || malformed.length != 1;
            if (expectedIsArray) {
                expectedDocument.startArray("field");
            } else {
                expectedDocument.field("field");
            }
            inputDocument.startArray("field");

            int valuesIdx = 0;
            int malformedIdx = 0;
            for (int j = 0; j < values.length + malformed.length; j++) {
                if (j < values.length) {
                    expectedDocument.value(values[j]);
                } else {
                    expectedDocument.value(malformed[j - values.length]);
                }

                if (valuesIdx == values.length) {
                    inputDocument.value(malformed[malformedIdx++]);
                } else if (malformedIdx == malformed.length) {
                    inputDocument.value(values[valuesIdx++]);
                } else {
                    if (randomBoolean()) {
                        inputDocument.value(values[valuesIdx++]);
                    } else {
                        inputDocument.value(malformed[malformedIdx++]);
                    }
                }
            }

            if (expectedIsArray) {
                expectedDocument.endArray();
            }
            expectedDocument.endObject();
            inputDocument.endArray().endObject();

            expectedDocuments.add(expectedDocument);
            inputDocuments.add(inputDocument);
        }

        var mapping = jsonBuilder().startObject().startObject("properties").startObject("field");
        minimalMapping(mapping);
        mapping.field("ignore_malformed", true).endObject().endObject().endObject();
        var indexService = createIndex(
            "test-index",
            Settings.builder().put("index.mapping.source.mode", "synthetic").put("index.mapping.synthetic_source_keep", "arrays").build(),
            mapping
        );
        for (int i = 0; i < inputDocuments.size(); i++) {
            var document = inputDocuments.get(i);
            var indexRequest = new IndexRequest("test-index");
            indexRequest.id("my-id-" + i);

            indexRequest.source(document);
            client().index(indexRequest).actionGet();
        }

        var refreshRequest = new RefreshRequest("test-index");
        client().execute(RefreshAction.INSTANCE, refreshRequest).actionGet();

        for (int i = 0; i < expectedDocuments.size(); i++) {
            var document = expectedDocuments.get(i);
            String expectedSource = Strings.toString(document);
            var searchRequest = new SearchRequest("test-index");
            searchRequest.source().query(new IdsQueryBuilder().addIds("my-id-" + i));
            var searchResponse = client().search(searchRequest).actionGet();
            try {
                var hit = searchResponse.getHits().getHits()[0];
                assertThat(hit.getId(), equalTo("my-id-" + i));
                assertThat(hit.getSourceAsString(), equalTo(expectedSource));
            } finally {
                searchResponse.decRef();
            }
        }
    }

    public void testSynthesizeRandomArrayInNestedContext() throws Exception {
        var arrayValues = new Object[randomIntBetween(1, 8)][randomIntBetween(2, 64)];
        for (int i = 0; i < arrayValues.length; i++) {
            for (int j = 0; j < arrayValues[i].length; j++) {
                arrayValues[i][j] = randomInt(10) == 0 ? null : getRandomValue();
            }
        }

        var mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("parent")
            .field("type", "nested")
            .startObject("properties")
            .startObject("field");
        minimalMapping(mapping);
        mapping.endObject().endObject().endObject().endObject().endObject();

        var indexService = createIndex(
            "test-index",
            Settings.builder().put("index.mapping.source.mode", "synthetic").put("index.mapping.synthetic_source_keep", "arrays").build(),
            mapping
        );

        var indexRequest = new IndexRequest("test-index");
        indexRequest.id("my-id-1");
        var source = jsonBuilder().startObject().startArray("parent");
        for (Object[] arrayValue : arrayValues) {
            source.startObject().array("field", arrayValue).endObject();
        }
        source.endArray().endObject();
        indexRequest.source(source);
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client().index(indexRequest).actionGet();

        var expectedSource = jsonBuilder().startObject();
        if (arrayValues.length > 1) {
            expectedSource.startArray("parent");
        } else {
            expectedSource.field("parent");
        }
        for (Object[] arrayValue : arrayValues) {
            expectedSource.startObject();
            expectedSource.array("field", arrayValue);
            expectedSource.endObject();
        }
        if (arrayValues.length > 1) {
            expectedSource.endArray();
        }
        expectedSource.endObject();
        var expected = Strings.toString(expectedSource);

        var searchRequest = new SearchRequest("test-index");
        searchRequest.source().query(new IdsQueryBuilder().addIds("my-id-1"));
        var searchResponse = client().search(searchRequest).actionGet();
        try {
            var hit = searchResponse.getHits().getHits()[0];
            assertThat(hit.getId(), equalTo("my-id-1"));
            assertThat(hit.getSourceAsString(), equalTo(expected));
        } finally {
            searchResponse.decRef();
        }

        assertThat(indexService.mapperService().mappingLookup().getMapper("parent.field").getOffsetFieldName(), nullValue());

        try (var searcher = indexService.getShard(0).acquireSearcher(getTestName())) {
            var reader = searcher.getDirectoryReader();
            var document = reader.storedFields().document(0);
            Set<String> storedFieldNames = new LinkedHashSet<>(document.getFields().stream().map(IndexableField::name).toList());
            assertThat(storedFieldNames, contains("_ignored_source"));
            assertThat(FieldInfos.getMergedFieldInfos(reader).fieldInfo("parent.field.offsets"), nullValue());
        }
    }

    protected void minimalMapping(XContentBuilder b) throws IOException {
        String fieldTypeName = getFieldTypeName();
        assertThat(fieldTypeName, notNullValue());
        b.field("type", fieldTypeName);
    }

    protected abstract String getFieldTypeName();

    protected abstract Object getRandomValue();

    protected abstract Object getMalformedValue();

    protected void verifySyntheticArray(Object[][] arrays) throws IOException {
        var mapping = jsonBuilder().startObject().startObject("properties").startObject("field");
        minimalMapping(mapping);
        mapping.endObject().endObject().endObject();
        verifySyntheticArray(arrays, mapping, "_id");
    }

    protected void verifySyntheticArray(Object[][] arrays, XContentBuilder mapping, String... expectedStoredFields) throws IOException {
        verifySyntheticArray(arrays, arrays, mapping, expectedStoredFields);
    }

    private XContentBuilder arrayToSource(Object[] array) throws IOException {
        var source = jsonBuilder().startObject();
        if (array != null) {
            source.startArray("field");
            for (Object arrayValue : array) {
                source.value(arrayValue);
            }
            source.endArray();
        } else {
            source.field("field").nullValue();
        }
        return source.endObject();
    }

    protected void verifySyntheticArray(
        Object[][] inputArrays,
        Object[][] expectedArrays,
        XContentBuilder mapping,
        String... expectedStoredFields
    ) throws IOException {
        assertThat(inputArrays.length, equalTo(expectedArrays.length));

        var indexService = createIndex(
            "test-index",
            Settings.builder().put("index.mapping.source.mode", "synthetic").put("index.mapping.synthetic_source_keep", "arrays").build(),
            mapping
        );
        for (int i = 0; i < inputArrays.length; i++) {
            var indexRequest = new IndexRequest("test-index");
            indexRequest.id("my-id-" + i);
            var inputSource = arrayToSource(inputArrays[i]);
            indexRequest.source(inputSource);
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client().index(indexRequest).actionGet();

            var expectedSource = arrayToSource(expectedArrays[i]);

            var searchRequest = new SearchRequest("test-index");
            searchRequest.source().query(new IdsQueryBuilder().addIds("my-id-" + i));
            var searchResponse = client().search(searchRequest).actionGet();
            try {
                var hit = searchResponse.getHits().getHits()[0];
                assertThat(hit.getId(), equalTo("my-id-" + i));
                assertThat(hit.getSourceAsString(), equalTo(Strings.toString(expectedSource)));
            } finally {
                searchResponse.decRef();
            }
        }

        try (var searcher = indexService.getShard(0).acquireSearcher(getTestName())) {
            var reader = searcher.getDirectoryReader();
            for (int i = 0; i < expectedArrays.length; i++) {
                var document = reader.storedFields().document(i);
                // Verify that there is no ignored source:
                Set<String> storedFieldNames = new LinkedHashSet<>(document.getFields().stream().map(IndexableField::name).toList());
                assertThat(storedFieldNames, contains(expectedStoredFields));
            }
            var fieldInfo = FieldInfos.getMergedFieldInfos(reader).fieldInfo("field.offsets");
            assertThat(fieldInfo.getDocValuesType(), equalTo(DocValuesType.SORTED));
        }
    }

    protected void verifySyntheticObjectArray(List<List<Object[]>> documents) throws IOException {
        var mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("object")
            .startObject("properties")
            .startObject("field");
        minimalMapping(mapping);
        mapping.endObject().endObject().endObject().endObject().endObject();
        var indexService = createIndex(
            "test-index",
            Settings.builder().put("index.mapping.source.mode", "synthetic").put("index.mapping.synthetic_source_keep", "arrays").build(),
            mapping
        );
        for (int i = 0; i < documents.size(); i++) {
            var document = documents.get(i);

            var indexRequest = new IndexRequest("test-index");
            indexRequest.id("my-id-" + i);
            var source = jsonBuilder().startObject();
            source.startArray("object");
            for (Object[] arrayValue : document) {
                source.startObject();
                source.array("field", arrayValue);
                source.endObject();
            }
            source.endArray().endObject();
            var expectedSource = Strings.toString(source);
            indexRequest.source(source);
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client().index(indexRequest).actionGet();

            var searchRequest = new SearchRequest("test-index");
            searchRequest.source().query(new IdsQueryBuilder().addIds("my-id-" + i));
            var searchResponse = client().search(searchRequest).actionGet();
            try {
                var hit = searchResponse.getHits().getHits()[0];
                assertThat(hit.getId(), equalTo("my-id-" + i));
                assertThat(hit.getSourceAsString(), equalTo(expectedSource));
            } finally {
                searchResponse.decRef();
            }
        }

        indexService.getShard(0).forceMerge(new ForceMergeRequest("test-index").maxNumSegments(1));
        try (var searcher = indexService.getShard(0).acquireSearcher(getTestName())) {
            var reader = searcher.getDirectoryReader();
            for (int i = 0; i < documents.size(); i++) {
                var document = reader.storedFields().document(i);
                // Verify that there is ignored source because of leaf array being wrapped by object array:
                List<String> storedFieldNames = document.getFields().stream().map(IndexableField::name).toList();
                assertThat(storedFieldNames, contains("_id", "_ignored_source"));

                // Verify that there is no offset field:
                LeafReader leafReader = reader.leaves().get(0).reader();
                for (FieldInfo fieldInfo : leafReader.getFieldInfos()) {
                    String name = fieldInfo.getName();
                    assertFalse("expected no field that contains [offsets] in name, but found [" + name + "]", name.contains("offsets"));
                }

                var binaryDocValues = leafReader.getBinaryDocValues("object.field.offsets");
                assertThat(binaryDocValues, nullValue());
            }
        }
    }

    protected void verifySyntheticArrayInObject(List<Object[]> documents) throws IOException {
        var mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("object")
            .startObject("properties")
            .startObject("field");
        minimalMapping(mapping);
        mapping.endObject().endObject().endObject().endObject().endObject();

        var indexService = createIndex(
            "test-index",
            Settings.builder().put("index.mapping.source.mode", "synthetic").put("index.mapping.synthetic_source_keep", "arrays").build(),
            mapping
        );
        for (int i = 0; i < documents.size(); i++) {
            var arrayValue = documents.get(i);

            var indexRequest = new IndexRequest("test-index");
            indexRequest.id("my-id-" + i);
            var source = jsonBuilder().startObject();
            source.startObject("object");
            source.array("field", arrayValue);
            source.endObject().endObject();
            var expectedSource = Strings.toString(source);
            indexRequest.source(source);
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client().index(indexRequest).actionGet();

            var searchRequest = new SearchRequest("test-index");
            searchRequest.source().query(new IdsQueryBuilder().addIds("my-id-" + i));
            var searchResponse = client().search(searchRequest).actionGet();
            try {
                var hit = searchResponse.getHits().getHits()[0];
                assertThat(hit.getId(), equalTo("my-id-" + i));
                assertThat(hit.getSourceAsString(), equalTo(expectedSource));
            } finally {
                searchResponse.decRef();
            }
        }

        indexService.getShard(0).forceMerge(new ForceMergeRequest("test-index").maxNumSegments(1));
        try (var searcher = indexService.getShard(0).acquireSearcher(getTestName())) {
            var reader = searcher.getDirectoryReader();
            for (int i = 0; i < documents.size(); i++) {
                var document = reader.storedFields().document(i);
                // Verify that there is no ignored source:
                Set<String> storedFieldNames = new LinkedHashSet<>(document.getFields().stream().map(IndexableField::name).toList());
                assertThat(storedFieldNames, contains("_id"));
            }
            var fieldInfo = FieldInfos.getMergedFieldInfos(reader).fieldInfo("object.field.offsets");
            assertThat(fieldInfo.getDocValuesType(), equalTo(DocValuesType.SORTED));
        }
    }

}
