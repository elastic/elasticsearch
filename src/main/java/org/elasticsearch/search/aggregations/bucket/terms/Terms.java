/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.support.ScriptValueType;

import java.util.Collection;
import java.util.Comparator;

/**
 *
 */
public interface Terms extends Aggregation, Iterable<Terms.Bucket> {

    static enum ValueType {

        STRING(ScriptValueType.STRING),
        LONG(ScriptValueType.LONG),
        DOUBLE(ScriptValueType.DOUBLE);

        final ScriptValueType scriptValueType;

        private ValueType(ScriptValueType scriptValueType) {
            this.scriptValueType = scriptValueType;
        }

        static ValueType resolveType(String type) {
            if ("string".equals(type)) {
                return STRING;
            }
            if ("double".equals(type) || "float".equals(type)) {
                return DOUBLE;
            }
            if ("long".equals(type) || "integer".equals(type) || "short".equals(type) || "byte".equals(type)) {
                return LONG;
            }
            return null;
        }
    }

    static interface Bucket extends Comparable<Bucket>, org.elasticsearch.search.aggregations.bucket.Bucket {

        Text getKey();

        Number getKeyAsNumber();
    }

    Collection<Bucket> buckets();

    Bucket getByTerm(String term);


    /**
     *
     */
    static abstract class Order implements ToXContent {

        /**
         * Order by the (higher) count of each term.
         */
        public static final Order COUNT_DESC = new InternalOrder((byte) 1, "_count", false, new Comparator<Terms.Bucket>() {
            @Override
            public int compare(Terms.Bucket o1, Terms.Bucket o2) {
                long i = o2.getDocCount() - o1.getDocCount();
                if (i == 0) {
                    i = o2.compareTo(o1);
                    if (i == 0) {
                        i = System.identityHashCode(o2) - System.identityHashCode(o1);
                    }
                }
                return i > 0 ? 1 : -1;
            }
        });

        /**
         * Order by the (lower) count of each term.
         */
        public static final Order COUNT_ASC = new InternalOrder((byte) 2, "_count", true, new Comparator<Terms.Bucket>() {

            @Override
            public int compare(Terms.Bucket o1, Terms.Bucket o2) {
                return -COUNT_DESC.comparator().compare(o1, o2);
            }
        });

        /**
         * Order by the terms.
         */
        public static final Order TERM_DESC = new InternalOrder((byte) 3, "_term", false, new Comparator<Terms.Bucket>() {

            @Override
            public int compare(Terms.Bucket o1, Terms.Bucket o2) {
                return o2.compareTo(o1);
            }
        });

        /**
         * Order by the terms.
         */
        public static final Order TERM_ASC = new InternalOrder((byte) 4, "_term", true, new Comparator<Terms.Bucket>() {

            @Override
            public int compare(Terms.Bucket o1, Terms.Bucket o2) {
                return -TERM_DESC.comparator().compare(o1, o2);
            }
        });

        /**
         * Creates a bucket ordering strategy which sorts buckets based on a single-valued calc get
         *
         * @param   aggregationName the name of the get
         * @param   asc             The direction of the order (ascending or descending)
         */
        public static InternalOrder aggregation(String aggregationName, boolean asc) {
            return new InternalOrder.Aggregation(aggregationName, null, asc);
        }

        /**
         * Creates a bucket ordering strategy which sorts buckets based on a multi-valued calc get
         *
         * @param   aggregationName the name of the get
         * @param   valueName       The name of the value of the multi-value get by which the sorting will be applied
         * @param   asc             The direction of the order (ascending or descending)
         */
        public static InternalOrder aggregation(String aggregationName, String valueName, boolean asc) {
            return new InternalOrder.Aggregation(aggregationName, valueName, asc);
        }


        protected abstract Comparator<Bucket> comparator();

    }
}
