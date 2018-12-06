/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlException;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.is;

public class FieldHitExtractorTests extends AbstractWireSerializingTestCase<FieldHitExtractor> {
    public static FieldHitExtractor randomFieldHitExtractor() {
        String hitName = randomAlphaOfLength(5);
        String name = randomAlphaOfLength(5) + "." + hitName;
        return new FieldHitExtractor(name, null, randomBoolean(), hitName);
    }

    @Override
    protected FieldHitExtractor createTestInstance() {
        return randomFieldHitExtractor();
    }

    @Override
    protected Reader<FieldHitExtractor> instanceReader() {
        return FieldHitExtractor::new;
    }

    @Override
    protected FieldHitExtractor mutateInstance(FieldHitExtractor instance) throws IOException {
        return new FieldHitExtractor(instance.fieldName() + "mutated", null, true, instance.hitName());
    }

    public void testGetDottedValueWithDocValues() {
        String grandparent = randomAlphaOfLength(5);
        String parent = randomAlphaOfLength(5);
        String child = randomAlphaOfLength(5);
        String fieldName = grandparent + "." + parent + "." + child;

        FieldHitExtractor extractor = new FieldHitExtractor(fieldName, null, true);

        int times = between(1, 1000);
        for (int i = 0; i < times; i++) {

            List<Object> documentFieldValues = new ArrayList<>();
            if (randomBoolean()) {
                documentFieldValues.add(randomValue());
            }

            SearchHit hit = new SearchHit(1);
            DocumentField field = new DocumentField(fieldName, documentFieldValues);
            hit.fields(singletonMap(fieldName, field));
            Object result = documentFieldValues.isEmpty() ? null : documentFieldValues.get(0);
            assertEquals(result, extractor.extract(hit));
        }
    }

    public void testGetDottedValueWithSource() throws Exception {
        String grandparent = randomAlphaOfLength(5);
        String parent = randomAlphaOfLength(5);
        String child = randomAlphaOfLength(5);
        String fieldName = grandparent + "." + parent + "." + child;

        FieldHitExtractor extractor = new FieldHitExtractor(fieldName, null, false);

        int times = between(1, 1000);
        for (int i = 0; i < times; i++) {
            /* We use values that are parsed from json as "equal" to make the
             * test simpler. */
            Object value = randomValue();
            SearchHit hit = new SearchHit(1);
            XContentBuilder source = JsonXContent.contentBuilder();
            boolean hasGrandparent = randomBoolean();
            boolean hasParent = randomBoolean();
            boolean hasChild = randomBoolean();
            boolean hasSource = hasGrandparent && hasParent && hasChild;

            source.startObject();
            if (hasGrandparent) {
                source.startObject(grandparent);
                if (hasParent) {
                    source.startObject(parent);
                    if (hasChild) {
                        source.field(child, value);
                        if (randomBoolean()) {
                            source.field(fieldName + randomAlphaOfLength(3), value + randomAlphaOfLength(3));
                        }
                    }
                    source.endObject();
                }
                source.endObject();
            }
            source.endObject();
            BytesReference sourceRef = BytesReference.bytes(source);
            hit.sourceRef(sourceRef);
            Object extract = extractor.extract(hit);
            assertEquals(hasSource ? value : null, extract);
        }
    }

    public void testGetDocValue() {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor extractor = new FieldHitExtractor(fieldName, null, true);

        int times = between(1, 1000);
        for (int i = 0; i < times; i++) {
            List<Object> documentFieldValues = new ArrayList<>();
            if (randomBoolean()) {
                documentFieldValues.add(randomValue());
            }
            SearchHit hit = new SearchHit(1);
            DocumentField field = new DocumentField(fieldName, documentFieldValues);
            hit.fields(singletonMap(fieldName, field));
            Object result = documentFieldValues.isEmpty() ? null : documentFieldValues.get(0);
            assertEquals(result, extractor.extract(hit));
        }
    }

    public void testGetDate() {
        long millis = 1526467911780L;
        List<Object> documentFieldValues = Collections.singletonList(Long.toString(millis));
        SearchHit hit = new SearchHit(1);
        DocumentField field = new DocumentField("my_date_field", documentFieldValues);
        hit.fields(singletonMap("my_date_field", field));
        FieldHitExtractor extractor = new FieldHitExtractor("my_date_field", DataType.DATE, true);
        assertEquals(DateUtils.of(millis), extractor.extract(hit));
    }

    public void testGetSource() throws IOException {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor extractor = new FieldHitExtractor(fieldName, null, false);

        int times = between(1, 1000);
        for (int i = 0; i < times; i++) {
            /* We use values that are parsed from json as "equal" to make the
             * test simpler. */
            Object value = randomValue();
            SearchHit hit = new SearchHit(1);
            XContentBuilder source = JsonXContent.contentBuilder();
            source.startObject(); {
                source.field(fieldName, value);
                if (randomBoolean()) {
                    source.field(fieldName + "_random_junk", value + "_random_junk");
                }
            }
            source.endObject();
            BytesReference sourceRef = BytesReference.bytes(source);
            hit.sourceRef(sourceRef);
            assertEquals(value, extractor.extract(hit));
        }
    }

    public void testToString() {
        assertEquals("hit.field@hit", new FieldHitExtractor("hit.field", null, true, "hit").toString());
    }

    public void testMultiValuedDocValue() {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor fe = new FieldHitExtractor(fieldName, null, true);
        SearchHit hit = new SearchHit(1);
        DocumentField field = new DocumentField(fieldName, asList("a", "b"));
        hit.fields(singletonMap(fieldName, field));
        SqlException ex = expectThrows(SqlException.class, () -> fe.extract(hit));
        assertThat(ex.getMessage(), is("Arrays (returned by [" + fieldName + "]) are not supported"));
    }

    public void testMultiValuedSourceValue() throws IOException {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor fe = new FieldHitExtractor(fieldName, null, false);
        SearchHit hit = new SearchHit(1);
        XContentBuilder source = JsonXContent.contentBuilder();
        source.startObject(); {
            source.field(fieldName, asList("a", "b"));
        }
        source.endObject();
        BytesReference sourceRef = BytesReference.bytes(source);
        hit.sourceRef(sourceRef);
        SqlException ex = expectThrows(SqlException.class, () -> fe.extract(hit));
        assertThat(ex.getMessage(), is("Arrays (returned by [" + fieldName + "]) are not supported"));
    }

    public void testSingleValueArrayInSource() throws IOException {
        String fieldName = randomAlphaOfLength(5);
        FieldHitExtractor fe = new FieldHitExtractor(fieldName, null, false);
        SearchHit hit = new SearchHit(1);
        XContentBuilder source = JsonXContent.contentBuilder();
        Object value = randomValue();
        source.startObject(); {
            source.field(fieldName, Collections.singletonList(value));
        }
        source.endObject();
        BytesReference sourceRef = BytesReference.bytes(source);
        hit.sourceRef(sourceRef);
        assertEquals(value, fe.extract(hit));
    }

    public void testExtractSourcePath() {
        FieldHitExtractor fe = new FieldHitExtractor("a.b.c", null, false);
        Object value = randomValue();
        Map<String, Object> map = singletonMap("a", singletonMap("b", singletonMap("c", value)));
        assertThat(fe.extractFromSource(map), is(value));
    }

    public void testExtractSourceIncorrectPath() {
        FieldHitExtractor fe = new FieldHitExtractor("a.b.c.d", null, false);
        Object value = randomNonNullValue();
        Map<String, Object> map = singletonMap("a", singletonMap("b", singletonMap("c", value)));
        SqlException ex = expectThrows(SqlException.class, () -> fe.extractFromSource(map));
        assertThat(ex.getMessage(), is("Cannot extract value [a.b.c.d] from source"));
    }

    public void testMultiValuedSource() {
        FieldHitExtractor fe = new FieldHitExtractor("a", null, false);
        Object value = randomValue();
        Map<String, Object> map = singletonMap("a", asList(value, value));
        SqlException ex = expectThrows(SqlException.class, () -> fe.extractFromSource(map));
        assertThat(ex.getMessage(), is("Arrays (returned by [a]) are not supported"));
    }

    public Object randomValue() {
        Supplier<Object> value = randomFrom(Arrays.asList(
                () -> randomAlphaOfLength(10),
                ESTestCase::randomLong,
                ESTestCase::randomDouble,
                () -> null));
        return value.get();
    }

    public Object randomNonNullValue() {
        Supplier<Object> value = randomFrom(Arrays.asList(
                () -> randomAlphaOfLength(10),
                ESTestCase::randomLong,
                ESTestCase::randomDouble));
        return value.get();
    }
}
