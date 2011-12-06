/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.apache.lucene.queryParser;

import gnu.trove.map.hash.TObjectFloatHashMap;

import java.util.List;

/**
 *
 */
public class MultiFieldQueryParserSettings extends QueryParserSettings {

    List<String> fields = null;
    TObjectFloatHashMap<String> boosts = null;
    float tieBreaker = 0.0f;
    boolean useDisMax = true;

    public List<String> fields() {
        return fields;
    }

    public void fields(List<String> fields) {
        this.fields = fields;
    }

    public TObjectFloatHashMap<String> boosts() {
        return boosts;
    }

    public void boosts(TObjectFloatHashMap<String> boosts) {
        this.boosts = boosts;
    }

    public float tieBreaker() {
        return tieBreaker;
    }

    public void tieBreaker(float tieBreaker) {
        this.tieBreaker = tieBreaker;
    }

    public boolean useDisMax() {
        return useDisMax;
    }

    public void useDisMax(boolean useDisMax) {
        this.useDisMax = useDisMax;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        // if there is a single field, its the same as single mapper parser / settings
        // we take for that also in the code
        if (fields == null || fields.size() == 1) return super.equals(o);

        if (!super.equals(o)) return false;

        MultiFieldQueryParserSettings that = (MultiFieldQueryParserSettings) o;

        if (Float.compare(that.tieBreaker, tieBreaker) != 0) return false;
        if (useDisMax != that.useDisMax) return false;
        if (boosts != null ? !boosts.equals(that.boosts) : that.boosts != null) return false;
        if (fields != null ? !fields.equals(that.fields) : that.fields != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        result = 31 * result + (boosts != null ? boosts.hashCode() : 0);
        result = 31 * result + (tieBreaker != +0.0f ? Float.floatToIntBits(tieBreaker) : 0);
        result = 31 * result + (useDisMax ? 1 : 0);
        return result;
    }
}
