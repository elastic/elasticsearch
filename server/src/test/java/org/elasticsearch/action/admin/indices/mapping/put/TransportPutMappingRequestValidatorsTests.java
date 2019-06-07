package org.elasticsearch.action.admin.indices.mapping.put;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;

public class TransportPutMappingRequestValidatorsTests extends ESTestCase {

    private final MappingRequestValidator EMPTY = (request, state, indices) -> null;
    private final MappingRequestValidator FAIL = (request, state, indices) -> new Exception("failure");

    public void testValidates() {
        final int numberOfValidations = randomIntBetween(0, 8);
        final List<MappingRequestValidator> validators = new ArrayList<>(numberOfValidations);
        for (int i = 0; i < numberOfValidations; i++) {
            validators.add(EMPTY);
        }
        final TransportPutMappingAction.RequestValidators requestValidators = new TransportPutMappingAction.RequestValidators(validators);
        assertNull(requestValidators.validateRequest(null, null, null));
    }

    public void testFailure() {
        final TransportPutMappingAction.RequestValidators validators = new TransportPutMappingAction.RequestValidators(List.of(FAIL));
        assertNotNull(validators.validateRequest(null, null, null));
    }

    public void testValidatesAfterFailure() {
        final TransportPutMappingAction.RequestValidators validators =
                new TransportPutMappingAction.RequestValidators(List.of(FAIL, EMPTY));
        assertNotNull(validators.validateRequest(null, null, null));
    }

    public void testMultipleFailures() {
        final int numberOfFailures = randomIntBetween(2, 8);
        final List<MappingRequestValidator> validators = new ArrayList<>(numberOfFailures);
        for (int i = 0; i < numberOfFailures; i++) {
            validators.add(FAIL);
        }
        final TransportPutMappingAction.RequestValidators requestValidators = new TransportPutMappingAction.RequestValidators(validators);
        final Exception e = requestValidators.validateRequest(null, null, null);
        assertNotNull(e);
        assertThat(e.getSuppressed(), Matchers.arrayWithSize(numberOfFailures - 1));
    }

    public void testRandom() {
        final int numberOfValidations = randomIntBetween(0, 8);
        final int numberOfFailures = randomIntBetween(0, 8);
        final List<MappingRequestValidator> validators = new ArrayList<>(numberOfValidations + numberOfFailures);
        for (int i = 0; i < numberOfValidations; i++) {
            validators.add(EMPTY);
        }
        for (int i = 0; i < numberOfFailures; i++) {
            validators.add(FAIL);
        }
        Randomness.shuffle(validators);
        final TransportPutMappingAction.RequestValidators requestValidators = new TransportPutMappingAction.RequestValidators(validators);
        final Exception e = requestValidators.validateRequest(null, null, null);
        if (numberOfFailures == 0) {
            assertNull(e);
        } else {
            assertNotNull(e);
        }
        assertThat(e.getSuppressed(), Matchers.arrayWithSize(numberOfFailures - 1));
    }

}