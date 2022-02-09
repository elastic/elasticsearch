/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.IngestDocument;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.ml.inference.results.NerResults.ENTITY_FIELD;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class NerResultsTests extends InferenceResultsTestCase<NerResults> {
    @Override
    protected Writeable.Reader<NerResults> instanceReader() {
        return NerResults::new;
    }

    @Override
    protected NerResults createTestInstance() {
        int numEntities = randomIntBetween(0, 3);

        return new NerResults(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            Stream.generate(
                () -> new NerResults.EntityGroup(
                    randomAlphaOfLength(10),
                    randomAlphaOfLength(10),
                    randomDouble(),
                    randomIntBetween(-1, 5),
                    randomIntBetween(5, 10)
                )
            ).limit(numEntities).collect(Collectors.toList()),
            randomBoolean()
        );
    }

    @SuppressWarnings("unchecked")
    public void testAsMap() {
        NerResults testInstance = createTestInstance();
        Map<String, Object> asMap = testInstance.asMap();
        List<Map<String, Object>> resultList = (List<Map<String, Object>>) asMap.get(ENTITY_FIELD);
        if (resultList == null) {
            return;
        }
        assertThat(resultList, hasSize(testInstance.getEntityGroups().size()));
        assertThat(asMap.get(testInstance.getResultsField()), equalTo(testInstance.getAnnotatedResult()));
        if (testInstance.isTruncated) {
            assertThat(asMap.get("is_truncated"), is(true));
        } else {
            assertThat(asMap, not(hasKey("is_truncated")));
        }
        for (int i = 0; i < testInstance.getEntityGroups().size(); i++) {
            NerResults.EntityGroup entity = testInstance.getEntityGroups().get(i);
            Map<String, Object> map = resultList.get(i);
            assertThat(map.get(NerResults.EntityGroup.CLASS_NAME), equalTo(entity.getClassName()));
            assertThat(map.get("entity"), equalTo(entity.getEntity()));
            assertThat(map.get(NerResults.EntityGroup.CLASS_PROBABILITY), equalTo(entity.getClassProbability()));
            Integer startPos = (Integer) map.get(NerResults.EntityGroup.START_POS);
            Integer endPos = (Integer) map.get(NerResults.EntityGroup.END_POS);
            if (startPos != null) {
                assertThat(startPos, equalTo(entity.getStartPos()));
            }
            if (endPos != null) {
                assertThat(endPos, equalTo(entity.getEndPos()));
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    void assertFieldValues(NerResults createdInstance, IngestDocument document, String resultsField) {
        assertThat(
            document.getFieldValue(resultsField + "." + createdInstance.getResultsField(), String.class),
            equalTo(createdInstance.getAnnotatedResult())
        );

        if (createdInstance.getEntityGroups().size() > 0) {
            List<Map<String, Object>> resultList = (List<Map<String, Object>>) document.getFieldValue(
                resultsField + "." + ENTITY_FIELD,
                List.class
            );
            assertThat(resultList.size(), equalTo(createdInstance.getEntityGroups().size()));
            for (int i = 0; i < createdInstance.getEntityGroups().size(); i++) {
                NerResults.EntityGroup entity = createdInstance.getEntityGroups().get(i);
                Map<String, Object> map = resultList.get(i);
                assertThat(map.get(NerResults.EntityGroup.CLASS_NAME), equalTo(entity.getClassName()));
                assertThat(map.get("entity"), equalTo(entity.getEntity()));
                assertThat(map.get(NerResults.EntityGroup.CLASS_PROBABILITY), equalTo(entity.getClassProbability()));
                Integer startPos = (Integer) map.get(NerResults.EntityGroup.START_POS);
                Integer endPos = (Integer) map.get(NerResults.EntityGroup.END_POS);
                if (startPos != null) {
                    assertThat(startPos, equalTo(entity.getStartPos()));
                }
                if (endPos != null) {
                    assertThat(endPos, equalTo(entity.getEndPos()));
                }
            }
        }
    }

}
