/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;

import java.util.List;

/**
 * Extend me to test a type conversion function that is lenient on the type of its input (see {@link ConvertFunction#lenientOnInputType}).
 * A signature of a single argument that the function doesn't take is not an error: the function resolves and evaluates to {@code null}.
 * <p>
 * It is still an error when the type can't be the type of a column a query returns ({@code date_period}, {@code short}...) or is
 * {@code unsupported}, and when the signature has more arguments, which are options or a base and are checked by their type as ever.
 * </p>
 */
public abstract class ErrorsForLenientConversionTestCase extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected boolean invalidSignatureResolvesToNull(List<DataType> signature) {
        return signature.size() == 1
            && DataType.isRepresentableInOutput(signature.getFirst())
            && signature.getFirst() != DataType.UNSUPPORTED;
    }
}
