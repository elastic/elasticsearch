/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class FieldCapabilitiesRequestTests extends AbstractWireSerializingTestCase<FieldCapabilitiesRequest> {

    @Override
    protected FieldCapabilitiesRequest createTestInstance() {
        FieldCapabilitiesRequest request =  new FieldCapabilitiesRequest();
        int size = randomIntBetween(1, 20);
        String[] randomFields = new String[size];
        for (int i = 0; i < size; i++) {
            randomFields[i] = randomAlphaOfLengthBetween(5, 10);
        }

        size = randomIntBetween(0, 20);
        String[] randomIndices = new String[size];
        for (int i = 0; i < size; i++) {
            randomIndices[i] = randomAlphaOfLengthBetween(5, 10);
        }
        request.fields(randomFields);
        request.indices(randomIndices);
        if (randomBoolean()) {
            request.indicesOptions(randomBoolean() ? IndicesOptions.strictExpand() : IndicesOptions.lenientExpandOpen());
        }
        request.includeUnmapped(randomBoolean());
        return request;
    }

    @Override
    protected Writeable.Reader<FieldCapabilitiesRequest> instanceReader() {
        return FieldCapabilitiesRequest::new;
    }

    @Override
    protected FieldCapabilitiesRequest mutateInstance(FieldCapabilitiesRequest instance) throws IOException {
        List<Consumer<FieldCapabilitiesRequest>> mutators = new ArrayList<>();
        mutators.add(request -> {
            String[] fields = ArrayUtils.concat(request.fields(), new String[] {randomAlphaOfLength(10)});
            request.fields(fields);
        });
        mutators.add(request -> {
            String[] indices = ArrayUtils.concat(instance.indices(), generateRandomStringArray(5, 10, false, false));
            request.indices(indices);
        });
        mutators.add(request -> {
            IndicesOptions indicesOptions = randomValueOtherThan(request.indicesOptions(),
                () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
            request.indicesOptions(indicesOptions);
        });
        mutators.add(request -> request.setMergeResults(request.isMergeResults() == false));
        mutators.add(request -> request.includeUnmapped(request.includeUnmapped() == false));

        FieldCapabilitiesRequest mutatedInstance = copyInstance(instance);
        Consumer<FieldCapabilitiesRequest> mutator = randomFrom(mutators);
        mutator.accept(mutatedInstance);
        return mutatedInstance;
    }

    public void testValidation() {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest()
            .indices("index2");
        ActionRequestValidationException exception = request.validate();
        assertNotNull(exception);
    }
}
