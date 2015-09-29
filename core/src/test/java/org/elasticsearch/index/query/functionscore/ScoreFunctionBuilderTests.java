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

import org.elasticsearch.index.query.functionscore.exp.ExponentialDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.fieldvaluefactor.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.lin.LinearDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.random.RandomScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionBuilder;
import org.elasticsearch.test.ESTestCase;

public class ScoreFunctionBuilderTests extends ESTestCase {

    public void testIllegalArguments() {
        try {
            new RandomScoreFunctionBuilder().seed(null);
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new ScriptScoreFunctionBuilder(null);
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new FieldValueFactorFunctionBuilder(null);
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new FieldValueFactorFunctionBuilder("").modifier(null);
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new GaussDecayFunctionBuilder(null, "", "", "");
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new GaussDecayFunctionBuilder("", "", null, "");
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new GaussDecayFunctionBuilder("", "", null, "", randomIntBetween(1, 100));
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new GaussDecayFunctionBuilder("", "", null, "", randomIntBetween(-100, -1));
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new LinearDecayFunctionBuilder(null, "", "", "");
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new LinearDecayFunctionBuilder("", "", null, "");
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new LinearDecayFunctionBuilder("", "", null, "", randomIntBetween(1, 100));
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new LinearDecayFunctionBuilder("", "", null, "", randomIntBetween(-100, -1));
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new ExponentialDecayFunctionBuilder(null, "", "", "");
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new ExponentialDecayFunctionBuilder("", "", null, "");
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new ExponentialDecayFunctionBuilder("", "", null, "", randomIntBetween(1, 100));
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new ExponentialDecayFunctionBuilder("", "", null, "", randomIntBetween(-100, -1));
            fail("must not be null");
        } catch(IllegalArgumentException e) {
            //all good
        }

    }
}
