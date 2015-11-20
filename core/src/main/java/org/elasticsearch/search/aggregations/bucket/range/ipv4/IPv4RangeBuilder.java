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

package org.elasticsearch.search.aggregations.bucket.range.ipv4;

import org.elasticsearch.search.aggregations.bucket.range.AbstractRangeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import static org.elasticsearch.index.mapper.ip.IpFieldMapper.cidrMaskToMinMax;

/**
 * Builder for the {@code IPv4Range} aggregation.
 */
public class IPv4RangeBuilder extends AbstractRangeBuilder<IPv4RangeBuilder> {

    /**
     * Sole constructor.
     */
    public IPv4RangeBuilder(String name) {
        super(name, InternalIPv4Range.TYPE.name());
    }

    /**
     * Add a new range to this aggregation.
     *
     * @param key  the key to use for this range in the response
     * @param from the lower bound on the distances, inclusive
     * @param to   the upper bound on the distances, exclusive
     */
    public IPv4RangeBuilder addRange(String key, String from, String to) {
        ranges.add(new Range(key, from, to));
        return this;
    }

    /**
     * Same as {@link #addMaskRange(String, String)} but uses the mask itself as a key.
     */
    public IPv4RangeBuilder addMaskRange(String mask) {
        return addMaskRange(mask, mask);
    }

    /**
     * Add a range based on a CIDR mask.
     */
    public IPv4RangeBuilder addMaskRange(String key, String mask) {
        long[] fromTo = cidrMaskToMinMax(mask);
        if (fromTo == null) {
            throw new SearchSourceBuilderException("invalid CIDR mask [" + mask + "] in ip_range aggregation [" + getName() + "]");
        }
        ranges.add(new Range(key, fromTo[0] < 0 ? null : fromTo[0], fromTo[1] < 0 ? null : fromTo[1]));
        return this;
    }

    /**
     * Same as {@link #addRange(String, String, String)} but the key will be
     * automatically generated.
     */
    public IPv4RangeBuilder addRange(String from, String to) {
        return addRange(null, from, to);
    }

    /**
     * Same as {@link #addRange(String, String, String)} but there will be no lower bound.
     */
    public IPv4RangeBuilder addUnboundedTo(String key, String to) {
        ranges.add(new Range(key, null, to));
        return this;
    }

    /**
     * Same as {@link #addUnboundedTo(String, String)} but the key will be
     * generated automatically.
     */
    public IPv4RangeBuilder addUnboundedTo(String to) {
        return addUnboundedTo(null, to);
    }

    /**
     * Same as {@link #addRange(String, String, String)} but there will be no upper bound.
     */
    public IPv4RangeBuilder addUnboundedFrom(String key, String from) {
        ranges.add(new Range(key, from, null));
        return this;
    }

    /**
     * Same as {@link #addUnboundedFrom(String, String)} but the key will be
     * generated automatically.
     */
    public IPv4RangeBuilder addUnboundedFrom(String from) {
        return addUnboundedFrom(null, from);
    }

}
