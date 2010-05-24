/*
 * Copyright 2010 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.util.http.collection;

import java.io.Serializable;

public class Pair<FirstType, SecondType> implements Serializable {
    private static final long serialVersionUID = -4403264592023348398L;
    private final FirstType firstValue;
    private final SecondType secondValue;

    public Pair(FirstType v1, SecondType v2) {
        firstValue = v1;
        secondValue = v2;
    }

    public FirstType getFirst() {
        return firstValue;
    }

    public SecondType getSecond() {
        return secondValue;
    }

    public String toString() {
        return "Pair(" + firstValue + ", " + secondValue + ")";
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Pair<?, ?> pair = (Pair<?, ?>) o;

        return (firstValue != null ? firstValue.equals(pair.firstValue) : pair.firstValue == null)
                && (secondValue != null ? secondValue.equals(pair.secondValue) : pair.secondValue == null);

    }

    public int hashCode() {
        int result;
        result = (firstValue != null ? firstValue.hashCode() : 0);
        result = 29 * result + (secondValue != null ? secondValue.hashCode() : 0);
        return result;
    }

    public static <K, V> Pair<K, V> of(K k, V v) {
        return new Pair<K, V>(k, v);
    }
}
