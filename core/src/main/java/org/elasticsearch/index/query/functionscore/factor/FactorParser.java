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

package org.elasticsearch.index.query.functionscore.factor;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParser;

import java.io.IOException;

/**
 *
 */
@Deprecated
public class FactorParser implements ScoreFunctionParser {

    public static String[] NAMES = { "boost_factor", "boostFactor" };

    @Inject
    public FactorParser() {
    }

    @Override
    public ScoreFunctionBuilder fromXContent(QueryParseContext parseContext, XContentParser parser) throws IOException, QueryParsingException {
        return new FactorBuilder().boostFactor(parser.floatValue());
    }

    @Override
    public String[] getNames() {
        return NAMES;
    }

    @Override
    public FactorBuilder getBuilderPrototype() {
        return FactorBuilder.PROTOTYPE;
    }
}
