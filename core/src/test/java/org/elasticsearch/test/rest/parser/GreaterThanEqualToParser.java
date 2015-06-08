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

package org.elasticsearch.test.rest.parser;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.rest.section.GreaterThanEqualToAssertion;

import java.io.IOException;

/**
 * Parser for gte assert sections
 */
public class GreaterThanEqualToParser implements RestTestFragmentParser<GreaterThanEqualToAssertion> {

    @Override
    public GreaterThanEqualToAssertion parse(RestTestSuiteParseContext parseContext) throws IOException, RestTestParseException {
        Tuple<String,Object> stringObjectTuple = parseContext.parseTuple();
        if (! (stringObjectTuple.v2() instanceof Comparable) ) {
            throw new RestTestParseException("gte section can only be used with objects that support natural ordering, found " + stringObjectTuple.v2().getClass().getSimpleName());
        }
        return new GreaterThanEqualToAssertion(stringObjectTuple.v1(), stringObjectTuple.v2());
    }
}
