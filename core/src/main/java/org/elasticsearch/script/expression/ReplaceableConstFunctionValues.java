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

package org.elasticsearch.script.expression;

import org.apache.lucene.queries.function.FunctionValues;

/**
 * A support class for an executable expression script that allows the double returned
 * by a {@link FunctionValues} to be modified.
 */
public class ReplaceableConstFunctionValues extends FunctionValues {
    private double value = 0;

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public double doubleVal(int doc) {
        return value;
    }

    @Override
    public String toString(int i) {
        return "ReplaceableConstFunctionValues: " + value;
    }
}
