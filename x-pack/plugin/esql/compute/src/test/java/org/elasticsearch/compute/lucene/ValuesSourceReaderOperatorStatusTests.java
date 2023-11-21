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

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.equalTo;

public class ValuesSourceReaderOperatorStatusTests extends AbstractWireSerializingTestCase<ValuesSourceReaderOperator.Status> {
    public static ValuesSourceReaderOperator.Status simple() {
        return new ValuesSourceReaderOperator.Status(Map.of("ReaderType", 3), 123);
    }

    public static String simpleToJson() {
        return """
            {"readers_built":{"ReaderType":3},"pages_processed":123}""";
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple()), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<ValuesSourceReaderOperator.Status> instanceReader() {
        return ValuesSourceReaderOperator.Status::new;
    }

    @Override
    public ValuesSourceReaderOperator.Status createTestInstance() {
        return new ValuesSourceReaderOperator.Status(randomReadersBuilt(), between(0, Integer.MAX_VALUE));
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
        switch (between(0, 1)) {
            case 0:
                return new ValuesSourceReaderOperator.Status(
                    randomValueOtherThan(instance.readersBuilt(), this::randomReadersBuilt),
                    instance.pagesProcessed()
                );
            case 1:
                return new ValuesSourceReaderOperator.Status(
                    instance.readersBuilt(),
                    randomValueOtherThan(instance.pagesProcessed(), () -> between(0, Integer.MAX_VALUE))
                );
            default:
                throw new UnsupportedOperationException();
        }
    }
}
