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

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.search.BoostAttribute;

import java.io.IOException;

public final class TypeToBoostTokenFilter extends TokenFilter {

    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final BoostAttribute boostAtt = addAttribute(BoostAttribute.class);
    private final float boostValue;
    private final String tokenType;

    protected TypeToBoostTokenFilter(TokenStream input, String tokenType, float boost) {
        super(input);
        this.tokenType = tokenType;
        this.boostValue = boost;
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (input.incrementToken()) {
            if (this.tokenType.equals(typeAtt.type())) {
                boostAtt.setBoost(boostValue);
            }
            return true;
        } else {
            return false;
        }
    }

}
