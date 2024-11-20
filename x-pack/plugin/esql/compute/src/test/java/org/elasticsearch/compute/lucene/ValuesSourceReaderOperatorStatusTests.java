/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.equalTo;

public class ValuesSourceReaderOperatorStatusTests extends AbstractWireSerializingTestCase<ValuesSourceReaderOperator.Status> {
    public static ValuesSourceReaderOperator.Status simple() {
        return new ValuesSourceReaderOperator.Status(Map.of("ReaderType", 3), 1022323, 123);
    }

    public static String simpleToJson() {
        return """
            {
              "readers_built" : {
                "ReaderType" : 3
              },
              "process_nanos" : 1022323,
              "process_time" : "1ms",
              "pages_processed" : 123
            }""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<ValuesSourceReaderOperator.Status> instanceReader() {
        return ValuesSourceReaderOperator.Status::new;
    }

    @Override
    public ValuesSourceReaderOperator.Status createTestInstance() {
        return new ValuesSourceReaderOperator.Status(randomReadersBuilt(), randomNonNegativeLong(), randomNonNegativeInt());
    }

    private Map<String, Integer> randomReadersBuilt() {
        int size = between(0, 10);
        Map<String, Integer> result = new TreeMap<>();
        while (result.size() < size) {
            result.put(randomAlphaOfLength(4), between(0, Integer.MAX_VALUE));
        }
        return result;
    }

    @Override
    protected ValuesSourceReaderOperator.Status mutateInstance(ValuesSourceReaderOperator.Status instance) throws IOException {
        Map<String, Integer> readersBuilt = instance.readersBuilt();
        long processNanos = instance.processNanos();
        int pagesProcessed = instance.pagesProcessed();
        switch (between(0, 2)) {
            case 0 -> readersBuilt = randomValueOtherThan(readersBuilt, this::randomReadersBuilt);
            case 1 -> processNanos = randomValueOtherThan(processNanos, ESTestCase::randomNonNegativeLong);
            case 2 -> pagesProcessed = randomValueOtherThan(pagesProcessed, ESTestCase::randomNonNegativeInt);
            default -> throw new UnsupportedOperationException();
        }
        return new ValuesSourceReaderOperator.Status(readersBuilt, processNanos, pagesProcessed);
    }
}
