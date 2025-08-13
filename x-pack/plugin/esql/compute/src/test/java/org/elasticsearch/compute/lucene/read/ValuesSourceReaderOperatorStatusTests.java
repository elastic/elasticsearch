/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.equalTo;

public class ValuesSourceReaderOperatorStatusTests extends AbstractWireSerializingTestCase<ValuesSourceReaderOperatorStatus> {
    public static ValuesSourceReaderOperatorStatus simple() {
        return new ValuesSourceReaderOperatorStatus(Map.of("ReaderType", 3), 1022323, 123, 200, 111, 222, 1000);
    }

    public static String simpleToJson() {
        return """
            {
              "readers_built" : {
                "ReaderType" : 3
              },
              "values_loaded" : 1000,
              "process_nanos" : 1022323,
              "process_time" : "1ms",
              "pages_received" : 123,
              "pages_emitted" : 200,
              "rows_received" : 111,
              "rows_emitted" : 222
            }""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<ValuesSourceReaderOperatorStatus> instanceReader() {
        return ValuesSourceReaderOperatorStatus::readFrom;
    }

    @Override
    public ValuesSourceReaderOperatorStatus createTestInstance() {
        return new ValuesSourceReaderOperatorStatus(
            randomReadersBuilt(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
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
    protected ValuesSourceReaderOperatorStatus mutateInstance(ValuesSourceReaderOperatorStatus instance) throws IOException {
        Map<String, Integer> readersBuilt = instance.readersBuilt();
        long processNanos = instance.processNanos();
        int pagesReceived = instance.pagesReceived();
        int pagesEmitted = instance.pagesEmitted();
        long rowsReceived = instance.rowsReceived();
        long rowsEmitted = instance.rowsEmitted();
        long valuesLoaded = instance.valuesLoaded();
        switch (between(0, 6)) {
            case 0 -> readersBuilt = randomValueOtherThan(readersBuilt, this::randomReadersBuilt);
            case 1 -> processNanos = randomValueOtherThan(processNanos, ESTestCase::randomNonNegativeLong);
            case 2 -> pagesReceived = randomValueOtherThan(pagesReceived, ESTestCase::randomNonNegativeInt);
            case 3 -> pagesEmitted = randomValueOtherThan(pagesEmitted, ESTestCase::randomNonNegativeInt);
            case 4 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 5 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            case 6 -> valuesLoaded = randomValueOtherThan(valuesLoaded, ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new ValuesSourceReaderOperatorStatus(
            readersBuilt,
            processNanos,
            pagesReceived,
            pagesEmitted,
            rowsReceived,
            rowsEmitted,
            valuesLoaded
        );
    }
}
