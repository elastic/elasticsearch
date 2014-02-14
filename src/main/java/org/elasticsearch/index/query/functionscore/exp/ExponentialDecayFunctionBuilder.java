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

package org.elasticsearch.index.query.functionscore.exp;


import org.elasticsearch.index.query.functionscore.DecayFunctionBuilder;

public class ExponentialDecayFunctionBuilder extends DecayFunctionBuilder {

    public ExponentialDecayFunctionBuilder(String fieldName, Object origin, Object scale) {
        super(fieldName, origin, scale);
    }

    @Override
    public String getName() {
        return ExponentialDecayFunctionParser.NAMES[0];
    }

}
