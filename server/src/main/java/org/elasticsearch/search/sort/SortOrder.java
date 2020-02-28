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

package org.elasticsearch.search.sort;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Comparator;
import java.util.Locale;

/**
 * A sorting order.
 *
 *
 */
public enum SortOrder implements Writeable {
    /**
     * Ascending order.
     */
    ASC {
        @Override
        public String toString() {
            return "asc";
        }

        @Override
        public int reverseMul() {
            return 1;
        }

        @Override
        public <T> Comparator<T> wrap(Comparator<T> delegate) {
            return delegate;
        }
    },
    /**
     * Descending order.
     */
    DESC {
        @Override
        public String toString() {
            return "desc";
        }

        @Override
        public int reverseMul() {
            return -1;
        }

        @Override
        public <T> Comparator<T> wrap(Comparator<T> delegate) {
            return delegate.reversed();
        }
    };

    public static SortOrder readFromStream(StreamInput in) throws IOException {
        return in.readEnum(SortOrder.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static SortOrder fromString(String op) {
        return valueOf(op.toUpperCase(Locale.ROOT));
    }

    /**
     * -1 if the sort is reversed from the standard comparators, 1 otherwise.
     */
    public abstract int reverseMul();

    /**
     * Wrap a comparator in one for this direction.
     */
    public abstract <T> Comparator<T> wrap(Comparator<T> delegate);
}
