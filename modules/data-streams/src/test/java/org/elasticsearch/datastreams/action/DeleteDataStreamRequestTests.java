/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction.Request;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DeleteDataStreamRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(TEST_REQUEST_TIMEOUT, randomArray(1, 3, String[]::new, () -> randomAlphaOfLength(6)));
    }

    @Override
    protected Request mutateInstance(Request instance) {
        // getNames() returns the live array, so copy it to leave the input untouched
        String[] names = instance.getNames().clone();
        boolean wildcardExpressionsOriginallySpecified = instance.isWildcardExpressionsOriginallySpecified();
        IndicesOptions indicesOptions = instance.indicesOptions();
        switch (between(0, 2)) {
            case 0 -> names = randomValueOtherThanMany(
                n -> Arrays.equals(n, instance.getNames()),
                () -> randomArray(1, 3, String[]::new, () -> randomAlphaOfLength(6))
            );
            case 1 -> wildcardExpressionsOriginallySpecified = wildcardExpressionsOriginallySpecified == false;
            case 2 -> indicesOptions = randomValueOtherThan(
                indicesOptions,
                () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return request(names, wildcardExpressionsOriginallySpecified, indicesOptions);
    }

    /**
     * Builds a request with any combination of names and wildcard flag. The flag is final and derived from
     * the names passed to the constructor, so the constructor is seeded with a wildcard or a concrete name to
     * get the wanted flag, then the real names are swapped in, the same way security rewrites wildcard
     * expressions into concrete names.
     */
    private static Request request(String[] names, boolean wildcardExpressionsOriginallySpecified, IndicesOptions indicesOptions) {
        Request request = new Request(TEST_REQUEST_TIMEOUT, wildcardExpressionsOriginallySpecified ? "*" : "concrete-name");
        request.indices(names);
        request.indicesOptions(indicesOptions);
        return request;
    }

    public void testValidateRequest() {
        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { "my-data-stream" });
        ActionRequestValidationException e = req.validate();
        assertNull(e);
    }

    public void testValidateRequestWithoutName() {
        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[0]);
        ActionRequestValidationException e = req.validate();
        assertNotNull(e);
        assertThat(e.validationErrors().size(), equalTo(1));
        assertThat(e.validationErrors().get(0), containsString("no data stream(s) specified"));
    }

}
