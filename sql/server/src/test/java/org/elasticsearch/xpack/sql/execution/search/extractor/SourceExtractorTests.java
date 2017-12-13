/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.function.Supplier;

public class SourceExtractorTests extends AbstractWireSerializingTestCase<SourceExtractor> {
    public static SourceExtractor randomSourceExtractor() {
        return new SourceExtractor(randomAlphaOfLength(5));
    }

    @Override
    protected SourceExtractor createTestInstance() {
        return randomSourceExtractor();
    }

    @Override
    protected Reader<SourceExtractor> instanceReader() {
        return SourceExtractor::new;
    }

    @Override
    protected SourceExtractor mutateInstance(SourceExtractor instance) throws IOException {
        return new SourceExtractor(instance.toString().substring(1) + "mutated");
    }

    public void testGet() throws IOException {
        String fieldName = randomAlphaOfLength(5);
        SourceExtractor extractor = new SourceExtractor(fieldName);
        
        int times = between(1, 1000);
        for (int i = 0; i < times; i++) {
            /* We use values that are parsed from json as "equal" to make the
             * test simpler. */
            @SuppressWarnings("unchecked")
            Supplier<Object> valueSupplier = randomFrom(
                    () -> randomAlphaOfLength(5),
                    () -> randomInt(),
                    () -> randomDouble());
            Object value = valueSupplier.get();
            SearchHit hit = new SearchHit(1);
            XContentBuilder source = JsonXContent.contentBuilder();
            source.startObject(); {
                source.field(fieldName, value);
                if (randomBoolean()) {
                    source.field(fieldName + "_random_junk", value + "_random_junk");
                }
            }
            source.endObject();
            BytesReference sourceRef = source.bytes();
            hit.sourceRef(sourceRef);
            assertEquals(value, extractor.get(hit));
        }
    }

    public void testToString() {
        assertEquals("#name", new SourceExtractor("name").toString());
    }
}
