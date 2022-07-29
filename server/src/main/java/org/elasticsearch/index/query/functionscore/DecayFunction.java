/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query.functionscore;

import org.apache.lucene.search.Explanation;

/**
 * Implement this interface to provide a decay function that is executed on a
 * distance. For example, this could be an exponential drop of, a triangle
 * function or something of the kind. This is used, for example, by
 * {@link GaussDecayFunctionBuilder}.
 *
 */
public interface DecayFunction {

    double evaluate(double value, double scale);

    Explanation explainFunction(String valueString, double value, double scale);

    /**
     * The final scale parameter is computed from the scale parameter given by
     * the user and a value. This value is the value that the decay function
     * should compute if document distance and user defined scale equal. The
     * scale parameter for the function must be adjusted accordingly in this
     * function
     *
     * @param scale
     *            the raw scale value given by the user
     * @param decay
     *            the value which decay function should take once the distance
     *            reaches this scale
     * */
    double processScale(double scale, double decay);
}
