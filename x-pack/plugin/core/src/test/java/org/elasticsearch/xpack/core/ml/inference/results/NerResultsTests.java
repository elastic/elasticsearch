/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class NerResultsTests extends AbstractWireSerializingTestCase<NerResults> {
    @Override
    protected Writeable.Reader<NerResults> instanceReader() {
        return NerResults::new;
    }

    @Override
    protected NerResults createTestInstance() {
        int numEntities = randomIntBetween(0, 3);
        List<NerResults.EntityGroup> entityGroups = new ArrayList<>();
        for (int i=0; i<numEntities; i++) {
            entityGroups.add(new NerResults.EntityGroup(randomFrom("foo", "bar"), randomDouble(), randomAlphaOfLength(4)));
        }
        return new NerResults(entityGroups);
    }

    @SuppressWarnings("unchecked")
    public void testAsMap() {
        NerResults testInstance = createTestInstance();
        Map<String, Object> asMap = testInstance.asMap();
        List<Map<String, Object>> resultList = (List<Map<String, Object>>)asMap.get("results");
        assertThat(resultList, hasSize(testInstance.getEntityGroups().size()));
        for (int i=0; i<testInstance.getEntityGroups().size(); i++) {
            NerResults.EntityGroup entity = testInstance.getEntityGroups().get(i);
            Map<String, Object> map = resultList.get(i);
            assertThat(map.get("label"), equalTo(entity.getLabel()));
            assertThat(map.get("score"), equalTo(entity.getScore()));
            assertThat(map.get("word"), equalTo(entity.getWord()));
        }
    }
}
