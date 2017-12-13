/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonMap;

public class DocValueExtractorTests extends AbstractWireSerializingTestCase<DocValueExtractor> {
    public static DocValueExtractor randomDocValueExtractor() {
        return new DocValueExtractor(randomAlphaOfLength(5));
    }

    @Override
    protected DocValueExtractor createTestInstance() {
        return randomDocValueExtractor();
    }

    @Override
    protected Reader<DocValueExtractor> instanceReader() {
        return DocValueExtractor::new;
    }

    @Override
    protected DocValueExtractor mutateInstance(DocValueExtractor instance) throws IOException {
        return new DocValueExtractor(instance.toString().substring(1) + "mutated");
    }

    public void testGet() {
        String fieldName = randomAlphaOfLength(5);
        DocValueExtractor extractor = new DocValueExtractor(fieldName);
        
        int times = between(1, 1000);
        for (int i = 0; i < times; i++) {
            List<Object> documentFieldValues = new ArrayList<>();
            documentFieldValues.add(new Object());
            if (randomBoolean()) {
                documentFieldValues.add(new Object());
            }
            SearchHit hit = new SearchHit(1);
            DocumentField field = new DocumentField(fieldName, documentFieldValues);
            hit.fields(singletonMap(fieldName, field));
            assertEquals(documentFieldValues.get(0), extractor.get(hit));
        }
    }

    public void testToString() {
        assertEquals("%incoming_links", new DocValueExtractor("incoming_links").toString());
    }
}
