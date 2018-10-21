/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
