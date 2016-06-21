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

package org.elasticsearch.painless;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ObjIntConsumer;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.regex.Matcher;

public class Augmentation {
    public static <T> int getLength(List<T> receiver) {
        return receiver.size();
    }
    
    public static String namedGroup(Matcher receiver, String name) {
        return receiver.group(name);
    }
    
    public static <T> boolean any(Iterable<T> receiver, Predicate<T> predicate) {
        for (T t : receiver) {
            if (predicate.test(t)) {
                return true;
            }
        }
        return false;
    }
    
    public static <T> int count(Iterable<T> receiver, Predicate<T> predicate) {
        int count = 0;
        for (T t : receiver) {
            if (predicate.test(t)) {
                count++;
            }
        }
        return count;
    }
    
    public static <T> Iterable<T> each(Iterable<T> receiver, Consumer<T> consumer) {
        receiver.forEach(consumer);
        return receiver;
    }
    
    public static <T> Iterable<T> eachWithIndex(Iterable<T> receiver, ObjIntConsumer<T> consumer) {
        int count = 0;
        for (T t : receiver) {
            consumer.accept(t, count++);
        }
        return receiver;
    }
    
    public static <T> boolean every(Iterable<T> receiver, Predicate<T> predicate) {
        for (T t : receiver) {
            if (predicate.test(t) == false) {
                return false;
            }
        }
        return true;
    }
    
    public static <T,U> List<U> findResults(Iterable<T> receiver, Function<T,U> filter) {
        List<U> list = new ArrayList<>();
        for (T t: receiver) {
           U result = filter.apply(t);
           if (result != null) {
               list.add(result);
           }
        }
        return list;
    }
    
    public static <T,U> Map<U,List<T>> groupBy(Iterable<T> receiver, Function<T,U> mapper) {
        Map<U,List<T>> map = new LinkedHashMap<>();
        for (T t : receiver) {
            U mapped = mapper.apply(t);
            List<T> results = map.get(mapped);
            if (results == null) {
                results = new ArrayList<>();
                map.put(mapped, results);
            }
            results.add(t);
        }
        return map;
    }
    
    public static <T> String join(Iterable<T> receiver, String separator) {
        StringBuilder sb = new StringBuilder();
        for (T t : receiver) {
            if (sb.length() > 0) {
                sb.append(separator);
            }
            sb.append(t);
        }
        return sb.toString();
    }
    
    public static <T> double sum(Iterable<T> receiver, ToDoubleFunction<T> function) {
        double sum = 0;
        for (T t : receiver) {
            sum += function.applyAsDouble(t);
        }
        return sum;
    }
}
